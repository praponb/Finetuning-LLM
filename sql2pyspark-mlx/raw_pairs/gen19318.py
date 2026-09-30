from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id", "product_id").orderBy("last_restock", "id").rowsBetween(-2, Window.currentRow)
w2 = Window.orderBy(F.col("sqft").desc_nulls_last())

with_dims = (
    df_inventory
    .alias("e")
    .join(df_stores.alias("b"), F.col("e.store_id") == F.col("b.id"), "left")
    .select("e.*", F.col("b.sqft"))
)

valid = (
    with_dims
    .filter(F.col("last_restock") >= F.lit("2023-02-23").cast("date"))
    .select(
        "id",
        "store_id",
        "product_id",
        "last_restock",
        "sqft",
        F.round((F.col("quantity") * 1.15), 2).alias("quantity_scaled"),
    )
)

scored = (
    valid
    .filter(F.col("quantity_scaled").between(38, 48))
    .withColumn("last3_max_sqft", F.max("sqft").over(w))
    .withColumn("last3_avg_sqft", F.avg("sqft").over(w))
    .withColumn("last3_count_sqft", F.count("sqft").over(w))
    .withColumn("last3_sum_sqft", F.sum("sqft").over(w))
    .withColumn("sqft_dense_rank", F.dense_rank().over(w2))
)

valid_inventory = (
    scored
    .filter(F.col("store_id").isin([5, 6]))
    .select(
        "*",
        (F.col("last3_avg_sqft") - F.col("last3_count_sqft")).alias("last3_avg_sqft_minus_last3_count_sqft"),
    )
)

result = (
    valid_inventory
    .groupBy(
        F.when(F.col("sqft") >= 16903, "PREMIUM").otherwise("STANDARD").alias("sqft_tier"),
    )
    .agg(
        F.count("*").alias("cnt"),
        F.count("*").alias("event_count"),
        F.max("last3_max_sqft").alias("max_last3_max_sqft"),
        F.round(F.avg("last3_max_sqft"), 2).alias("avg_last3_max_sqft"),
    )
    .orderBy(F.col("avg_last3_max_sqft").desc_nulls_first())
)
