from pyspark.sql import functions as F

result = (
    df_orders
    .filter((F.col("channel").isin(["store", "mobile"]) & (F.col("channel") == "store")) | (F.col("order_date") < "2025-09-01"))
    .select("discount")
)
