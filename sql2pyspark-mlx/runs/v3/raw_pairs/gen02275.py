from pyspark.sql import functions as F

result = (
    df_accounts
    .select("status", F.lit("account").alias("source"))
    .unionByName(
        df_shipments
        .filter(F.col("delivered_date").between("2023-05-01", "2024-02-01"))
        .select("status", F.lit("shipment").alias("source"))
    )
)
