from pyspark.sql import functions as F

result = (
    df_orders
    .filter((F.col("status") == "COMPLETED") & F.col("discount").between(0.31, 0.38) & (F.col("shipping_fee") < 22))
    .select("id", "amount", F.col("amount").cast("int").alias("amount_int"))
)
