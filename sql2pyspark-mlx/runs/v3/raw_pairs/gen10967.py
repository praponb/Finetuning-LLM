from pyspark.sql import functions as F

result = (
    df_accounts
    .select("status", F.lit("account").alias("source"))
    .unionByName(
        df_payments
        .filter(F.col("paid_at") < "2023-03-01")
        .select("status", F.lit("payment").alias("source"))
    )
    .distinct()
    .orderBy("source")
)
