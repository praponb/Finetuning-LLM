from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("customer_id") == 27)
    .select("status", "discount")
    .orderBy(F.col("discount").desc())
)
