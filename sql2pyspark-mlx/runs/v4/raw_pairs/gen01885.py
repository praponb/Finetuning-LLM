from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("method").isin(["bank_transfer", "card", "promptpay"]))
    .select("status")
    .unionByName(
        df_subscriptions
        .filter(F.col("start_date").between("2024-06-03", "2025-05-01"))
        .select("status")
    )
)
