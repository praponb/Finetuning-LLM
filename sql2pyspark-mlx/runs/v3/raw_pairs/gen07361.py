from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.year("opened_date") >= 2025)
    .select("status", F.lit("account").alias("source"))
    .unionByName(
        df_payments
        .filter(F.col("order_id") == 19)
        .select("status", F.lit("payment").alias("source"))
    )
)
