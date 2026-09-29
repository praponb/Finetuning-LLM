from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("channel") == "web")
    .select("status", F.lit("order").alias("source"))
    .unionByName(
        df_accounts
        .filter(F.col("customer_id").isin([21, 23]))
        .select("status", F.lit("account").alias("source"))
    )
)
