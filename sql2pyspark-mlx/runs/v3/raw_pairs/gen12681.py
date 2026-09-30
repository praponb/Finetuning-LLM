from pyspark.sql import functions as F

result = (
    df_orders
    .select("status", F.lit("order").alias("source"))
    .unionByName(
        df_accounts
        .filter(F.col("opened_date") <= "2023-03-27")
        .select("status", F.lit("account").alias("source"))
    )
    .distinct()
)
