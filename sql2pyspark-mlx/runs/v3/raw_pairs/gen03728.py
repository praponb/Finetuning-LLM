from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("status") != "CANCELLED")
    .select("status", "discount", "shipping_fee")
    .orderBy(F.col("status").desc(), F.col("discount").desc())
)
