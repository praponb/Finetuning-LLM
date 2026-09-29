from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.hour("paid_at").between(6, 14))
    .select("id", "status", "method")
    .unionByName(
        df_payments
        .filter(F.col("method").isin(["bank_transfer", "paypal"]))
        .select("id", "status", "method")
    )
    .orderBy(F.col("id").desc())
)
