from pyspark.sql import functions as F

result = (
    df_accounts
    .select("status", F.lit("account").alias("kind"))
    .unionByName(df_shipments.select("status", F.lit("shipment").alias("kind")))
    .distinct()
    .orderBy("kind")
)
