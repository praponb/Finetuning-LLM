from pyspark.sql import functions as F

result = (
    df_orders
    .select("id", "store_id", F.month("order_date").alias("order_date_month"))
    .orderBy(F.col("order_date_month").desc(), F.col("store_id").desc())
)
