from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("store_id").isin([2, 5, 22, 32]))
    .select("status", F.lit("order").alias("source"))
    .unionByName(
        df_accounts
        .filter(F.col("account_type").isin(["CHECKING", "LOAN", "SAVINGS"]))
        .select("status", F.lit("account").alias("source"))
    )
    .orderBy("status")
)
