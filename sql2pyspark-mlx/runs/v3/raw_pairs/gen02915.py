from pyspark.sql import functions as F

result = (
    df_orders
    .select("customer_id")
    .orderBy(F.col("customer_id").desc())
)
