from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy(F.col("opened_date").desc_nulls_last(), "id")
w2 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.last_restock").desc(), F.col("c.id").desc())
w_recent = Window.partitionBy("region").orderBy("sqft_per_quantity", "id").rowsBetween(-2, Window.currentRow)

not_flagged = (
    df_stores
    .alias("s")
    .join(df_inventory.alias("x"), (F.col("x.store_id") == F.col("s.id")) & (F.col("x.last_restock") < F.col("s.opened_date")), "left_semi")
)

deduped = (
    not_flagged
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
)

latest_match = (
    deduped
    .alias("s")
    .join(df_inventory.alias("c"), (F.col("c.store_id") == F.col("s.id")) & (F.col("c.last_restock") <= F.col("s.opened_date")), "inner")
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select("s.id", "s.region", "s.sqft", F.col("c.quantity"))
)

base = (
    latest_match
    .filter(F.col("region").isNotNull())
    .withColumn("sqft_per_quantity", (F.col("sqft") / F.nullif("quantity", F.lit(0))))
    .withColumn("region_lower", F.lower("region"))
)

enriched = (
    base
    .filter(F.col("quantity") != 150)
    .select(
        "id",
        "sqft",
        "quantity",
        F.avg("quantity").over(w_recent).alias("last3_avg_quantity"),
        F.min("quantity").over(w_recent).alias("last3_min_quantity"),
        F.sum("quantity").over(w_recent).alias("last3_sum_quantity"),
        F.count("quantity").over(w_recent).alias("last3_count_quantity"),
    )
)

valid = (
    enriched
    .filter(F.col("sqft") < 6383)
    .withColumn("sqft_plus_quantity", (F.col("sqft") + F.col("quantity")))
    .withColumn("quantity_rounded", F.round("quantity", 2))
)

result = (
    valid
    .select(
        "id",
        "sqft",
        "quantity",
        "last3_avg_quantity",
        "last3_min_quantity",
        "last3_sum_quantity",
        "sqft_plus_quantity",
        "quantity_rounded",
    )
    .orderBy(F.col("last3_sum_quantity").desc(), "quantity")
)
