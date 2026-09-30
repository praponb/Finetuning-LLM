from pyspark.sql import functions as F

src = (
    df_orders
    .filter(((F.col("store_id") == 6) & (F.col("store_id") == 40)) | (F.col("order_date") > "2024-02-01"))
    .withColumn("amount_int", F.col("amount").cast("int"))
)

order_quarter_summary = (
    src
    .groupBy(F.quarter("order_date").alias("order_quarter"))
    .agg(F.count(F.when(F.col("channel") == "store", 1)).alias("store_count"))
)

result = order_quarter_summary.orderBy(F.col("store_count").desc())
