from pyspark.sql import functions as F

result = (
    df_accounts
    .select("opened_date", F.lit("account").alias("source"))
    .unionByName(
        df_shipments
        .filter(F.col("shipped_date").between("2024-01-01", "2024-07-01"))
        .select(
        F.col("delivered_date").alias("opened_date"),
        F.lit("shipment").alias("source"),
    )
    )
    .distinct()
    .orderBy("source")
)
