from pyspark.sql import functions as F

result = (
    df_orders
    .filter((F.col("channel") == "store") & ((F.col("store_id") == 30) | (F.col("status") != "CANCELLED")))
    .select("id", "store_id")
)
