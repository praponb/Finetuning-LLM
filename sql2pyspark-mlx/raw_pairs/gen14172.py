from pyspark.sql import functions as F

result = (
    df_accounts
    .select(F.lit("account").alias("origin"), "status")
    .unionByName(
        df_payments
        .filter(F.col("status") == "REFUNDED")
        .select(F.lit("payment").alias("origin"), "status")
    )
)
