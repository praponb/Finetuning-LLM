from pyspark.sql import functions as F

result = (
    df_orders
    .select("status", F.lit("order").alias("source"))
    .unionByName(
        df_shipments
        .filter(F.col("delivered_date").between("2023-04-01", "2024-04-01"))
        .select("status", F.lit("shipment").alias("source"))
    )
)
