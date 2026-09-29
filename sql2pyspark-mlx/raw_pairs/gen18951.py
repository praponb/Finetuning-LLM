from pyspark.sql import functions as F

joined = (
    df_order_items
    .alias("src")
    .join(df_orders.alias("o"), F.col("src.order_id") == F.col("o.id"), "left")
    .select("src.*", F.col("o.shipping_fee"), F.col("o.order_date"), F.col("o.status"))
)

with_dims = (
    joined
    .alias("src")
    .join(df_products.alias("p"), F.col("src.product_id") == F.col("p.id"), "left")
    .select(
        "src.id",
        "src.order_id",
        "src.product_id",
        "src.order_date",
        "src.status",
        F.col("p.price"),
    )
)

remaining_keys = (
    with_dims
    .select("order_id")
    .intersect(
        df_order_items
        .filter(F.col("order_id").isin([15, 37]))
        .select("order_id")
    )
)

reconciled = (
    with_dims
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.order_id") == F.col("k.order_id")), "inner")
    .select("s.*")
)

joined_order_items = (
    reconciled
    .alias("e")
    .join(df_orders.alias("b"), F.col("e.order_id") == F.col("b.id"), "inner")
    .select("e.*", F.col("b.status").alias("order_status"))
)

joined_order_items_2 = (
    joined_order_items
    .alias("src")
    .join(df_products.alias("b"), F.col("src.product_id") == F.col("b.id"), "left")
    .select("src.*", F.col("b.stock_qty"), F.col("b.category"), F.col("b.cost"))
)

totals = (
    joined_order_items_2
    .withColumn("week_start", F.date_trunc("WEEK", "order_date"))
    .groupingSets(
        [["category", "week_start"], ["category"], []],
        "category",
        "week_start",
    )
    .agg(
        F.percentile_approx("price", [0.25, 0.5, 0.75]).alias("price_quartiles"),
        F.count(F.when(F.col("status") == "COMPLETED", 1)).alias("completed_count"),
        F.count("*").alias("event_count"),
        F.grouping("category").alias("grouping_level"),
    )
    .filter(F.col("event_count") < 1000)
)

result = (
    totals
    .select(
        "category",
        "week_start",
        "price_quartiles",
        "completed_count",
        "event_count",
        "grouping_level",
        F.when(F.col("category").rlike("^(Sports|Electronics)$"), "GROUP_B").when(F.col("category").rlike("^(Home|Books)$"), "GROUP_A").otherwise("EXTENDED").alias("category_zone"),
    )
)
