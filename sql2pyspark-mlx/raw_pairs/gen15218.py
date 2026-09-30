from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.quantity").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("week_start").orderBy(F.col("avg_order_item_unit_price_2").desc())

with_latest = (
    df_products
    .alias("s")
    .join(df_order_items.alias("c"), (F.col("c.product_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.product_name",
        "s.price",
        "s.stock_qty",
        "s.launch_date",
        "s.tags",
        F.col("c.unit_price").alias("order_item_unit_price"),
        F.col("c.quantity").alias("order_item_quantity"),
    )
)

latest_match = (
    with_latest
    .alias("s")
    .join(df_order_items.alias("c"), (F.col("c.product_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.unit_price").alias("order_item_unit_price_2"))
)

base = (
    latest_match
    .filter(F.col("launch_date") >= F.lit("2023-08-20").cast("date"))
    .withColumn("order_item_unit_price_minus_order_item_quantity", (F.col("order_item_unit_price") - F.col("order_item_quantity")))
)

cleaned = (
    base
    .filter(F.col("launch_date") >= F.to_date(F.lit("2023-09-04")))
    .select(
        "id",
        "price",
        "launch_date",
        "tags",
        "order_item_unit_price",
        "order_item_quantity",
        "order_item_unit_price_2",
        "order_item_unit_price_minus_order_item_quantity",
        F.round("price", 1).alias("price_rounded"),
    )
)

daily = (
    cleaned
    .groupBy(
        F.date_trunc("WEEK", "launch_date").alias("week_start"),
        F.date_trunc("MONTH", "launch_date").alias("activity_month"),
        F.date_trunc("QUARTER", "launch_date").alias("quarter_start"),
    )
    .agg(
        F.percentile_approx("order_item_unit_price_minus_order_item_quantity", [0.25, 0.5, 0.75]).alias("order_item_unit_price_minus_order_item_quantity_quartiles"),
        F.count("*").alias("cnt"),
        F.max("order_item_unit_price_minus_order_item_quantity").alias("max_order_item_unit_price_minus_order_item_quantity"),
        F.round(F.avg("order_item_unit_price_2"), 2).alias("avg_order_item_unit_price_2"),
    )
    .withColumn("avg_order_item_unit_price_2_dense_rank", F.dense_rank().over(w2))
)

result = (
    daily
    .select(
        "week_start",
        "activity_month",
        "quarter_start",
        "max_order_item_unit_price_minus_order_item_quantity",
        "avg_order_item_unit_price_2_dense_rank",
    )
    .orderBy(
        F.col("avg_order_item_unit_price_2_dense_rank").desc_nulls_first(),
        "week_start",
    )
)
