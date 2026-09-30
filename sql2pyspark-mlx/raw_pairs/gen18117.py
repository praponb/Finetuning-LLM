from pyspark.sql import functions as F

result = (
    df_orders
    .select("status", F.lit("order").alias("origin"))
    .unionByName(
        df_accounts
        .filter(F.col("account_type") != "SAVINGS")
        .select("status", F.lit("account").alias("origin"))
    )
)
