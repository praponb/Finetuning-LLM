from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("customer_id").isin([9, 36]))
    .select("opened_date", F.lit("account").alias("source"))
    .union(
        df_shipments
        .filter(F.col("status") != "DELIVERED")
        .select("delivered_date", F.lit("shipment").alias("source"))
    )
    .distinct()
)
