from pyspark.sql import functions as F

result = (
    df_orders
    .filter(~F.col("status").isin(["SHIPPED", "COMPLETED", "RETURNED"]))
    .select("shipping_fee", "amount")
    .orderBy(F.col("shipping_fee").desc())
)
