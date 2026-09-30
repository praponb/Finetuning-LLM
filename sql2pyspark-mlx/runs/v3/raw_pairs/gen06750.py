from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(((F.col("product_id") == 40) & F.col("quantity").between(4, 7)) | F.col("unit_price").between(120, 240))
    .select(
        "id",
        "quantity",
        "unit_price",
        (F.col("unit_price") - F.col("quantity")).alias("line_total"),
        F.coalesce("unit_price", F.lit(0)).alias("unit_price_filled"),
    )
)
