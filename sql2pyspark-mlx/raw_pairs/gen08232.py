from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.last_restock").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("city").orderBy("sqft", "id").rowsBetween(-5, -1)
w3 = Window.orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.orderBy("sqft_filled", "id")
w5 = Window.partitionBy("city")
w6 = Window.partitionBy("region").orderBy(F.col("opened_date").desc(), "id")

cleaned = (
    df_stores
    .filter((F.col("sqft") != 7153) & F.col("region").isNotNull())
    .select(
        "id",
        "store_name",
        "city",
        "region",
        "opened_date",
        "sqft",
        F.coalesce("sqft", F.lit(0)).alias("sqft_filled"),
    )
)

with_latest = (
    cleaned
    .alias("s")
    .join(df_inventory.alias("c"), (F.col("c.store_id") == F.col("s.id")) & (F.col("c.last_restock") <= F.col("s.opened_date")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.quantity"))
)

enriched = (
    with_latest
    .select(
        "*",
        F.avg("sqft_filled").over(w2).alias("sqft_filled_avg_5_prior"),
        F.sum("quantity").over(w3).alias("cumulative_quantity"),
        F.lead("quantity", 1).over(w4).alias("next_quantity"),
        F.round((F.col("sqft") / F.sum("sqft").over(w5)), 4).alias("sqft_share"),
        F.row_number().over(w6).alias("seq_no"),
    )
)

remaining_keys = (
    enriched
    .select("id")
    .subtract(
        df_stores
        .filter(F.col("sqft") < 5923)
        .select("id")
    )
)

reconciled = (
    enriched
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

kept = (
    reconciled
    .alias("s")
    .join(df_inventory.alias("ch"), (F.col("ch.store_id") == F.col("s.id")) & (F.col("ch.last_restock") <= F.col("s.opened_date")), "left_semi")
)

base = (
    kept
    .filter(F.col("opened_date") >= F.to_date(F.lit("2023-07-29")))
    .select(
        "*",
        F.upper(F.trim("store_name")).alias("store_name_normalized"),
        F.least("quantity", "sqft").alias("least_quantity_sqft"),
        F.coalesce("region", F.lit("NONE")).alias("region_filled"),
    )
)

result = (
    base
    .select(
        "id",
        "store_name",
        "city",
        "region",
        "opened_date",
        "sqft",
        "sqft_filled_avg_5_prior",
        "cumulative_quantity",
        "sqft_share",
        F.when(F.col("region").rlike("^(Central|East)$"), "EXTENDED").when(F.col("region").rlike("^(North|South)$"), "GROUP_A").otherwise("PRIORITY").alias("region_zone"),
    )
)
