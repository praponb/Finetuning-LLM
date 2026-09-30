from pyspark.sql import functions as F

result = (
    df_accounts
    .select("status")
    .intersect(df_payments.filter(F.col("order_id").isNotNull()).select("status"))
    .orderBy(F.col("status").desc())
)
