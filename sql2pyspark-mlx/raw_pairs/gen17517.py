from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.last_restock").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("city").orderBy("quantity")

kept = (
    df_stores
    .alias("s")
    .join(df_orders.alias("c"), (F.col("c.store_id") == F.col("s.id")) & (F.col("c.order_date") <= F.col("s.opened_date")), "left_semi")
)

valid = (
    kept
    .filter(F.col("region").isNotNull())
    .select(
        "id",
        "city",
        "region",
        "opened_date",
        "sqft",
        F.substring("store_name", 1, 4).alias("store_name_prefix"),
        F.coalesce("region", F.lit("DIRECT")).alias("region_clean"),
    )
)

not_flagged = (
    valid
    .alias("s")
    .join(df_orders.alias("x"), (F.col("x.store_id") == F.col("s.id")) & (F.col("x.amount") > 630), "left_semi")
)

latest_match = (
    not_flagged
    .alias("s")
    .join(df_inventory.alias("c"), (F.col("c.store_id") == F.col("s.id")) & (F.col("c.last_restock") <= F.col("s.opened_date")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.city",
        "s.opened_date",
        "s.sqft",
        "s.region_clean",
        F.col("c.last_restock"),
        F.col("c.quantity"),
    )
)

deduped = (
    latest_match
    .withColumn("latest_rank", F.rank().over(w2))
    .filter(F.col("latest_rank") == 1)
    .select("id", "city", "opened_date", "sqft", "region_clean", "last_restock")
)

grouped = (
    deduped
    .groupBy(
        F.when(F.col("sqft") >= 16146, "LARGE").otherwise("SMALL").alias("sqft_tier"),
    )
    .agg(
        F.percentile_approx("sqft", 0.9).alias("sqft_p90"),
        F.percentile_approx("sqft", [0.25, 0.5, 0.75]).alias("sqft_quartiles"),
        F.bit_or("sqft").alias("sqft_bit_or"),
        F.round(F.sum("sqft") / F.nullif(F.count(F.when(F.col("city") == "London", 1)), F.lit(0)), 2).alias("sqft_per_london"),
        F.count(F.when(F.col("city") == "Singapore", 1)).alias("singapore_rows"),
    )
)

result = (
    grouped
    .select(
        "sqft_tier",
        "sqft_p90",
        "sqft_quartiles",
        "sqft_bit_or",
        "sqft_per_london",
        "singapore_rows",
    )
    .orderBy(F.col("sqft_bit_or").desc_nulls_last(), F.col("singapore_rows").desc())
)
