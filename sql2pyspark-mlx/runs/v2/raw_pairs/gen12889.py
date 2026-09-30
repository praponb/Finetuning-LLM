from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("order_date") >= "2024-08-01")
    .select("id", "store_id")
)
