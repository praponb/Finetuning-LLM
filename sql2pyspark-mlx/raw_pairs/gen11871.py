from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy("adjusted_unit_price_whole", "id")
w_recent = Window.partitionBy("product_id").orderBy("stock_qty", "id")
w3 = Window.partitionBy("category").orderBy("adjusted_unit_price", "id").rowsBetween(-6, -1)
w_grp = Window.partitionBy("product_id").orderBy(F.col("adjusted_unit_price").desc_nulls_last())

filtered = (
    df_order_items
    .filter(F.col("quantity") < 7)
    .select(
        "id",
        "product_id",
        "unit_price",
        F.round((F.col("unit_price") * 100), 2).alias("adjusted_unit_price"),
        F.when(F.col("quantity") >= 6, "High").when(F.col("quantity") >= 3, "Medium").otherwise("Low").alias("quantity_tier"),
    )
)

cleaned = (
    filtered
    .filter(F.col("product_id").isNotNull() & (F.col("adjusted_unit_price") < 58))
    .withColumn("adjusted_unit_price_whole", F.floor("adjusted_unit_price"))
    .withColumn("unit_price_filled", F.coalesce("unit_price", F.lit(0)))
)

joined = (
    cleaned
    .alias("s")
    .join(df_products.alias("b"), F.col("s.product_id") == F.col("b.id"), "left")
    .select("s.*", F.col("b.category"), F.col("b.stock_qty"))
)

with_windows = (
    joined
    .select(
        "*",
        F.lag("stock_qty").over(w).alias("prev_stock_qty"),
        F.lead("unit_price_filled").over(w).alias("next_unit_price_filled"),
        F.lag("unit_price_filled").over(w).alias("prev_unit_price_filled"),
        F.row_number().over(w).alias("event_seq"),
    )
)

result = (
    with_windows
    .select(
        "*",
        F.lead("next_unit_price_filled", 1).over(w_recent).alias("next_next_unit_price_filled"),
        F.lag("prev_stock_qty", 1, 0).over(w_recent).alias("prev_prev_stock_qty"),
        F.avg("unit_price_filled").over(w3).alias("unit_price_filled_avg_6_prior"),
        F.rank().over(w_grp).alias("adjusted_unit_price_rank"),
    )
)
