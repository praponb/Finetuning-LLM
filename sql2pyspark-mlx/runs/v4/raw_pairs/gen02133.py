from pyspark.sql import functions as F

result = (
    df_accounts
    .select("status", F.lit("account").alias("source"))
    .unionByName(df_shipments.select("status", F.lit("shipment").alias("source")))
)
