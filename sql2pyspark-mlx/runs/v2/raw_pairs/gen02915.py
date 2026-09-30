from pyspark.sql import functions as F

result = (
    df_accounts
    .select("status", F.lit("account").alias("source"))
    .unionByName(
        df_shipments
        .filter(F.col("delivered_date").between("2024-06-08", "2025-07-01"))
        .select("status", F.lit("shipment").alias("source"))
    )
)
