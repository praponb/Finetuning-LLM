from pyspark.sql import functions as F

result = (
    df_orders
    .filter(((F.col("discount") < 0.15) & F.col("amount").between(1190, 1270)) | (F.col("status") == "CANCELLED"))
    .select("id", "shipping_fee")
)
