from pyspark.sql import functions as F

base = (
    df_order_items
    .filter(F.col("product_id").isNull())
    .select("*", F.col("unit_price").cast("int").alias("unit_price_int"))
)

result = (
    base
    .select("id")
    .intersect(
        df_order_items
        .filter(F.col("product_id").isin([7, 16, 19, 37]))
        .select("id")
    )
)
