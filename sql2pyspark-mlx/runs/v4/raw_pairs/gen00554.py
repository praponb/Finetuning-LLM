from pyspark.sql import functions as F

result = (
    df_shipments
    .select("cost")
    .intersect(
        df_order_items
        .filter(F.col("product_id").isNotNull())
        .select(F.col("unit_price").alias("cost"))
    )
)
