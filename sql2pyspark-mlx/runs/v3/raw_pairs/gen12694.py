from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("paid_at") >= "2023-10-01")
    .select("method")
    .unionByName(df_payments.filter(F.col("paid_at") < "2023-03-22").select("method"))
    .orderBy(F.col("method").desc())
)
