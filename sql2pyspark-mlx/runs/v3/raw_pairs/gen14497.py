from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("cost").between(30, 43))
    .select("shipped_date", F.lit("shipment").alias("source"))
    .unionByName(
        df_orders
        .select(F.col("order_date").alias("shipped_date"), F.lit("order").alias("source"))
    )
    .distinct()
    .orderBy(F.col("source").desc())
)
