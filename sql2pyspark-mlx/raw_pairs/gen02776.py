from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.unit_price").desc(), F.col("c.id").desc())

filtered = (
    df_products
    .filter((F.size("tags") >= 2) & (F.col("brand").eqNullSafe(F.lit("Initech"))))
    .select(
        "*",
        F.array_contains("tags", "sale").alias("is_sale"),
        F.coalesce("brand", F.lit("OTHER")).alias("resolved_brand"),
    )
)

result = (
    filtered
    .alias("s")
    .join(df_order_items.alias("c"), (F.col("c.product_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.product_name",
        "s.category",
        "s.price",
        "s.stock_qty",
        "s.launch_date",
        "s.tags",
        "s.resolved_brand",
        F.col("c.quantity").alias("order_item_quantity"),
    )
)
