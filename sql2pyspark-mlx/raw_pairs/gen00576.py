from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.unit_price").desc(), F.col("c.id").desc())

kept = (
    df_products
    .alias("s")
    .join(df_order_items.alias("x"), (F.col("x.product_id") == F.col("s.id")) & (F.col("x.product_id").isNull()), "left_anti")
)

with_latest = (
    kept
    .alias("s")
    .join(df_order_items.alias("c"), (F.col("c.product_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.category",
        "s.brand",
        "s.price",
        "s.stock_qty",
        "s.launch_date",
        F.col("c.quantity"),
    )
)

cleaned = (
    with_latest
    .filter((F.col("launch_date") >= F.to_date(F.lit("2023-06-25"))) & (~F.col("brand").eqNullSafe(F.lit("Globex"))))
    .withColumn("category_brand", F.concat("category", F.lit(" "), "brand"))
    .withColumn("brand_clean", F.coalesce("brand", F.lit("DIRECT")))
)

eligible = (
    cleaned
    .alias("s")
    .join(df_order_items.alias("x"), (F.col("x.product_id") == F.col("s.id")) & (F.col("x.order_id").isin([9, 27, 35])), "left_anti")
)

stats = (
    eligible
    .groupBy(
        F.when(F.col("stock_qty") >= 444, "PREMIUM").otherwise("STANDARD").alias("stock_qty_tier"),
    )
    .agg(
        F.bit_or("stock_qty").alias("stock_qty_bit_or"),
        F.corr("price", "stock_qty").alias("corr_price_stock_qty"),
        F.count_if(F.col("brand") == "Stark").alias("stark_events"),
    )
)

result = (
    stats
    .select(
        "stock_qty_tier",
        "stock_qty_bit_or",
        "corr_price_stock_qty",
        "stark_events",
    )
    .orderBy(F.col("corr_price_stock_qty").desc())
)
