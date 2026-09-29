from pyspark.sql import functions as F

result = (
    df_order_items
    .alias("a")
    .join(df_products.alias("p"), F.col("a.product_id").eqNullSafe(F.col("p.id")), "left")
    .select(
        "a.id",
        "a.order_id",
        "a.product_id",
        "a.unit_price",
        F.col("p.product_name"),
    )
)
