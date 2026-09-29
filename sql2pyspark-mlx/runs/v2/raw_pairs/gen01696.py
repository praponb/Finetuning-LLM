from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("status").isin(["PENDING", "SHIPPED", "COMPLETED"]))
    .select("status", F.lit("order").alias("source"))
    .unionByName(df_accounts.select("status", F.lit("account").alias("source")))
    .distinct()
    .orderBy(F.col("status").desc())
)
