from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("amount") >= 560)
    .select("id", "store_id", F.last_day("order_date").alias("order_date_month_end"))
)
