from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("status").isin(["PENDING", "COMPLETED"]) & (F.col("discount").isNotNull() | (F.col("status") == "PENDING")))
    .groupBy("status", F.date_format("order_date", "yyyy-MM").alias("order_month"))
    .agg(F.countDistinct("discount").alias("unique_statuss"))
    .orderBy(F.col("status").desc())
)
