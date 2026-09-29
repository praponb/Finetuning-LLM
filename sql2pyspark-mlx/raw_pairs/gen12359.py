from pyspark.sql import functions as F

result = (
    df_accounts
    .select(F.lit("account").alias("origin"), "status")
    .unionByName(
        df_shipments
        .filter(F.col("shipped_date").between("2024-01-29", "2024-12-01"))
        .select(F.lit("shipment").alias("origin"), "status")
    )
    .distinct()
    .orderBy("status")
)
