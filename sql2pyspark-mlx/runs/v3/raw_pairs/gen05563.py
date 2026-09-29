from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("status").isin(["DELIVERED", "LOST"]) & F.col("order_id").isin([18, 21, 24, 30]))
    .select(
        "id",
        "shipped_date",
        F.concat_ws("/", "carrier", "status").alias("carrier_status_key"),
        F.when(F.col("carrier").isin(["UPS", "Flash"]), "Priority").otherwise("Other").alias("carrier_group"),
    )
)
