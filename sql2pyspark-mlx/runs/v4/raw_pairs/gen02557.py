from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("amount").isNotNull())
    .select("id", "order_id")
    .intersect(df_payments.filter(F.col("method").isNull()).select("id", "order_id"))
    .orderBy("order_id")
)
