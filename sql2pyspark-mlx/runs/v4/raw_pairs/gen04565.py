from pyspark.sql import functions as F

filtered = (
    df_order_items
    .filter(F.col("quantity") >= 8)
    .select("*", F.col("unit_price").cast("int").alias("unit_price_int"))
)

result = (
    filtered
    .select("product_id", "id")
    .subtract(
        df_order_items
        .filter(F.col("unit_price") < 180)
        .select("product_id", "id")
    )
)
