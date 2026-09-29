from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("account_type") != "CHECKING")
    .select("status", F.lit("account").alias("source"))
    .unionByName(
        df_shipments
        .filter(F.col("shipped_date") >= "2025-06-01")
        .select("status", F.lit("shipment").alias("source"))
    )
    .distinct()
    .orderBy("source")
)
