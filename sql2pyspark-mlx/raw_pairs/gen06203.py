from pyspark.sql import functions as F

result = (
    df_orders
    .filter((F.col("customer_id") == 16) & (F.col("channel").isin(["web", "store"]) | F.col("customer_id").isin([6, 27, 32])))
    .groupBy("channel")
    .agg(
        F.count("discount").alias("discount_count"),
        F.countDistinct("discount").alias("unique_channels"),
    )
    .filter(F.col("unique_channels") > 5)
    .drop("unique_channels")
    .orderBy(F.col("channel").desc())
)
