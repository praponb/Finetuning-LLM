from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("method").isin(["bank_transfer", "paypal"]))
    .select("method")
    .unionByName(df_payments.filter(F.col("order_id").isNotNull()).select("method"))
    .distinct()
    .orderBy(F.col("method").desc())
)
