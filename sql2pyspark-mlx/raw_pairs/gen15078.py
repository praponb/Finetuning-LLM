from pyspark.sql import functions as F

result = (
    df_accounts
    .select(F.lit("account").alias("origin"), "status")
    .unionByName(
        df_shipments
        .filter(F.col("cost") < 31)
        .select(F.lit("shipment").alias("origin"), "status")
    )
)
