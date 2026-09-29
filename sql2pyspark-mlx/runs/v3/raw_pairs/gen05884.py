from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.to_date("paid_at") < "2024-04-21")
    .select("id", "status")
    .unionByName(
        df_payments
        .filter(F.col("method") == "bank_transfer")
        .select("id", "status")
    )
    .distinct()
    .orderBy("status")
)
