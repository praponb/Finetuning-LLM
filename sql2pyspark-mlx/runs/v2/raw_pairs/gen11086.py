from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("method") != "promptpay")
    .select("status", "method")
    .unionByName(
        df_payments
        .filter(F.col("method").isin(["bank_transfer", "paypal"]))
        .select("status", "method")
    )
    .orderBy("status")
)
