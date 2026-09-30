from pyspark.sql import functions as F

result = (
    df_orders
    .select("channel", F.date_format("order_date", "yyyy-MM").alias("order_date_label"))
)
