from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("status") == "OPEN")
    .select("status", F.lit("account").alias("source"))
    .unionByName(df_shipments.select("status", F.lit("shipment").alias("source")))
    .distinct()
)
