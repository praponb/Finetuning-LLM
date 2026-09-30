from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("method").isNull())
    .select("id", "paid_at")
    .orderBy("paid_at")
)
