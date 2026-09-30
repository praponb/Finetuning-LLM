from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("status") != "RETURNED")
    .select("id", "discount", "customer_id")
)
