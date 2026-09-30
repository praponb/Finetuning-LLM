from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("status").isin(["FROZEN", "OPEN"]))
    .select("status", F.lit("account").alias("source"))
    .unionByName(
        df_payments
        .filter(F.col("method").isin(["card", "bank_transfer"]))
        .select("status", F.lit("payment").alias("source"))
    )
    .orderBy("source")
)
