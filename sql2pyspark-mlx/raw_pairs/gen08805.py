from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id", "product_id").orderBy(F.col("quantity").desc_nulls_last())
w2 = Window.partitionBy("city").orderBy(F.col("sqft").desc(), "id")

valid = (
    df_inventory
    .filter(F.col("last_restock") >= F.lit("2023-07-17").cast("date"))
    .select(
        "id",
        "store_id",
        "product_id",
        "quantity",
        F.when(F.col("quantity") >= 270, "High").when(F.col("quantity") >= 106, "Medium").otherwise("Low").alias("quantity_tier"),
        F.round("quantity", 2).alias("quantity_rounded"),
    )
)

latest = (
    valid
    .withColumn("latest_rank", F.rank().over(w))
    .filter(F.col("latest_rank") == 1)
    .select("id", "store_id")
)

with_dims = (
    latest
    .alias("e")
    .join(df_stores.alias("b"), F.col("e.store_id") == F.col("b.id"), "left")
    .select("e.*", F.col("b.store_name"), F.col("b.sqft"), F.col("b.city"))
)

deduped = (
    with_dims
    .withColumn("dedup_rank", F.row_number().over(w2))
    .filter(F.col("dedup_rank") == 1)
)

result = (
    deduped
    .groupBy(
        F.when(F.col("sqft") >= 14614, "HIGH").otherwise("LOW").alias("sqft_tier"),
        "store_id",
    )
    .agg(
        F.count_if(F.col("city") == "Chiang Mai").alias("chiang_mai_events"),
        F.sum(F.when(F.col("city") == "Berlin", F.col("sqft"))).alias("berlin_total_sqft"),
        F.round(F.avg("sqft"), 2).alias("avg_sqft"),
    )
    .orderBy("berlin_total_sqft")
)
