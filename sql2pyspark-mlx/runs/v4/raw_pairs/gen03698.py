from pyspark.sql import functions as F

result = (
    df_accounts
    .select("status", F.lit("account").alias("source"))
    .unionByName(
        df_shipments
        .filter(F.col("delivered_date") > "2023-07-01")
        .select("status", F.lit("shipment").alias("source"))
    )
    .distinct()
    .orderBy(F.col("status").desc())
)
