from pyspark.sql import functions as F

result = (
    df_orders
    .select("status", F.lit("order").alias("kind"))
    .unionByName(df_accounts.select("status", F.lit("account").alias("kind")))
)
