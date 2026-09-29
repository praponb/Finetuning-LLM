from pyspark.sql import functions as F

result = (
    df_orders
    .select("status", F.lit("order").alias("record_type"))
    .unionByName(df_accounts.select("status", F.lit("account").alias("record_type")))
    .distinct()
    .orderBy("status")
)
