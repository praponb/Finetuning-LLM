from pyspark.sql import functions as F

result = (
    df_order_items
    .filter((F.col("quantity") > 5) & F.col("unit_price").isNull())
    .groupBy("order_id")
    .agg(F.count("*").alias("total_count"))
    .filter(F.col("total_count") > 4)
    .orderBy(F.col("order_id").desc())
)
