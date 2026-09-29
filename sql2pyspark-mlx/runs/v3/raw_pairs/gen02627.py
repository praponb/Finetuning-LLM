from pyspark.sql import functions as F

result = (
    df_orders
    .filter((F.col("channel") == "web") & (F.col("amount") > 1230))
    .select("customer_id")
)
