from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("customer_id") == 25)
    .select("status", F.lit("order").alias("source"))
    .unionByName(
        df_shipments
        .filter(F.col("carrier") == "DHL")
        .select("status", F.lit("shipment").alias("source"))
    )
)
