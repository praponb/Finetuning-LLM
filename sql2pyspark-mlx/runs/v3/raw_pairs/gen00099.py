from pyspark.sql import functions as F

result = (
    df_order_items
    .groupBy(
        F.when(F.col("quantity") >= 8, "high").when(F.col("quantity") >= 4, "mid").otherwise("low").alias("quantity_bucket"),
    )
    .agg(
        F.avg("unit_price").alias("avg_unit_price"),
        F.count("*").alias("cnt"),
    )
    .filter(F.col("avg_unit_price") < 190)
)
