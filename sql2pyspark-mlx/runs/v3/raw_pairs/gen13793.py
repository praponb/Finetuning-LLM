from pyspark.sql import functions as F

result = (
    df_orders
    .select("status", F.lit("order").alias("source"))
    .unionByName(df_accounts.select("status", F.lit("account").alias("source")))
    .orderBy(F.col("source").desc())
)
