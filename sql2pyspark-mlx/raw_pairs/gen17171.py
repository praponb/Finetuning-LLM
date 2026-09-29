from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.unit_price").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("supplier_id").orderBy("order_item_unit_price", "id")
w3 = Window.partitionBy(F.year("launch_date"), "supplier_id").orderBy("cost", "id")
w4 = Window.orderBy(F.col("price").desc(), "id")
w5 = Window.partitionBy(F.date_trunc("MONTH", "launch_date"), "supplier_id").orderBy("cost", "id")
w6 = Window.partitionBy("supplier_id")

filtered = (
    df_products
    .filter(F.month("launch_date").isin([1, 5, 10]))
    .select(
        "*",
        (F.col("cost") - F.col("price")).alias("margin"),
        F.coalesce("brand", F.lit("OTHER")).alias("brand_clean"),
    )
)

with_latest = (
    filtered
    .alias("s")
    .join(df_order_items.alias("c"), (F.col("c.product_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.product_name",
        "s.price",
        "s.cost",
        "s.launch_date",
        "s.tags",
        "s.supplier_id",
        "s.brand_clean",
        F.col("c.unit_price").alias("order_item_unit_price"),
    )
)

scored = (
    with_latest
    .select(
        "*",
        F.row_number().over(w2).alias("event_seq"),
        F.ntile(4).over(w2).alias("order_item_unit_price_bucket"),
        F.lead("cost", 1).over(w3).alias("next_cost"),
    )
)

with_windows = (
    scored
    .filter(F.col("order_item_unit_price") <= 260)
    .select(
        "*",
        F.ntile(4).over(w4).alias("price_bucket"),
        F.lag("next_cost", 2).over(w5).alias("prev_next_cost"),
        F.round((F.col("order_item_unit_price_bucket") / F.sum("order_item_unit_price_bucket").over(w6)), 4).alias("order_item_unit_price_bucket_share"),
        F.round((F.col("cost") / F.sum("cost").over(w6)), 4).alias("cost_share"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "product_name",
        "price",
        "cost",
        "launch_date",
        "brand_clean",
        "event_seq",
        "price_bucket",
        "prev_next_cost",
    )
    .orderBy(F.col("prev_next_cost").desc())
)
