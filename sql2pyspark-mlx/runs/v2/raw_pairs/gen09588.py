from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("customer_id") == 39)
    .select("status", F.lit("account").alias("source"))
    .unionByName(
        df_shipments
        .filter(F.col("cost") >= 41)
        .select("status", F.lit("shipment").alias("source"))
    )
    .distinct()
)
