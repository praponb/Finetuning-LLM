from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("order_id").isin([3, 9]))
    .select("status")
    .unionByName(df_payments.filter(F.col("order_id").isNotNull()).select("status"))
    .orderBy(F.col("status").desc())
)
