from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("shipping_fee").between(18, 27))
    .select("status", F.lit("order").alias("source"))
    .unionByName(
        df_shipments
        .filter(F.col("delivered_date") < "2024-07-24")
        .select("status", F.lit("shipment").alias("source"))
    )
    .distinct()
)
