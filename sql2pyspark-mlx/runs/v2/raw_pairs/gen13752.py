from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("quantity") != 6)
    .groupBy(
        F.when(F.col("quantity") >= 7, "high").when(F.col("quantity") >= 2, "mid").otherwise("low").alias("quantity_bucket"),
    )
    .agg(F.avg("quantity").alias("avg_quantity"))
    .filter(F.col("avg_quantity") < 7.5)
    .orderBy("quantity_bucket")
)
