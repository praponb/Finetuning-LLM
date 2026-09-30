from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "inner")
    .filter(F.col("o.discount").isNotNull())
    .select(
        F.col("s.carrier"),
        F.col("s.cost"),
        F.col("s.shipped_date"),
        F.col("o.channel"),
        F.col("o.store_id"),
    )
    .orderBy(F.col("shipped_date").desc())
)
