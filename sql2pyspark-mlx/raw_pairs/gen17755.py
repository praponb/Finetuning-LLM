from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("status") != "SHIPPED")
    .groupBy("status")
    .agg(F.count("*").alias("row_count"))
    .orderBy("row_count", "status")
    .limit(3)
)
