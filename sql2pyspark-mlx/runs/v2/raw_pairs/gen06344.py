from pyspark.sql import functions as F

result = (
    df_accounts
    .select("status", F.lit("account").alias("source"))
    .unionByName(
        df_shipments
        .filter(F.col("order_id").isin([1, 7, 13]))
        .select("status", F.lit("shipment").alias("source"))
    )
)
