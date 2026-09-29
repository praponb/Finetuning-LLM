from pyspark.sql import functions as F

result = (
    df_orders
    .filter((F.col("shipping_fee") >= 34) | (F.col("status") != "COMPLETED"))
    .select("id", "status", "discount")
)
