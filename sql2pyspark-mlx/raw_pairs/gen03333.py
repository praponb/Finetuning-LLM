from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id", "product_id").orderBy(F.col("quantity_filled").desc(), "id")
w2 = Window.orderBy(F.col("quantity_filled").desc())
w3 = Window.partitionBy("product_id")
w4 = Window.partitionBy("product_id", "order_id").orderBy("quantity_filled", "id")

filtered = (
    df_order_items
    .filter(F.col("product_id").isNotNull())
    .select(
        "id",
        "order_id",
        "product_id",
        "unit_price",
        F.coalesce("quantity", F.lit(0)).alias("quantity_filled"),
        F.col("quantity").cast("string").alias("quantity_str"),
    )
)

result = (
    filtered
    .select(
        "*",
        F.row_number().over(w).alias("row_seq"),
        F.cume_dist().over(w2).alias("quantity_filled_cume_dist"),
        F.round((F.col("unit_price") / F.sum("unit_price").over(w3)), 4).alias("unit_price_share"),
        F.row_number().over(w4).alias("seq_no"),
        F.ntile(4).over(w4).alias("quantity_filled_bucket"),
    )
)
