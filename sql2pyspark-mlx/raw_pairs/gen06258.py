from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("unit_price") < 140)
    .agg(F.count("*").alias("total_count"))
    .orderBy(F.col("total_count").desc())
)
