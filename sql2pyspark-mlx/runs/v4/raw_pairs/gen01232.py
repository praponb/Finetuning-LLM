from pyspark.sql import functions as F

result = (
    df_accounts
    .select(F.col("opened_date").alias("event_date"), F.lit("account").alias("source"))
    .unionByName(
        df_shipments
        .filter(F.col("delivered_date") <= "2025-04-12")
        .select(
        F.col("delivered_date").alias("event_date"),
        F.lit("shipment").alias("source"),
    )
    )
    .orderBy("event_date")
)
