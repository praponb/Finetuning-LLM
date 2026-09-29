from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("quantity").desc_nulls_first())
w2 = Window.partitionBy("product_id").orderBy("quantity")
w3 = Window.partitionBy("product_id").orderBy(F.col("quantity_cume_dist").desc())
w4 = Window.partitionBy("product_id").orderBy("quantity_dense_rank")
w5 = Window.orderBy(F.col("quantity_cume_dist").desc_nulls_first())
w6 = Window.orderBy(F.col("line_total").asc_nulls_first())
w7 = Window.partitionBy("product_id").orderBy(F.col("quantity_cume_dist").asc_nulls_last())
unit_price_by_product_id = df_order_items.groupBy("product_id").agg(F.sum("unit_price").alias("sum_order_item_unit_price"))

remaining_keys = (
    df_order_items
    .select("id", "order_id")
    .subtract(
        df_order_items
        .filter(F.col("unit_price") >= 290)
        .select("id", "order_id")
    )
)

verified = (
    df_order_items
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.order_id") == F.col("k.order_id")), "inner")
    .select("s.*")
)

filtered = (
    verified
    .filter(F.col("product_id").isNotNull())
    .select(
        "id",
        "order_id",
        "product_id",
        "quantity",
        "unit_price",
        (F.col("unit_price") * F.col("quantity")).alias("line_total"),
    )
)

enriched = (
    filtered
    .select(
        "product_id",
        "line_total",
        F.cume_dist().over(w).alias("quantity_cume_dist"),
        F.dense_rank().over(w2).alias("quantity_dense_rank"),
    )
)

with_windows = (
    enriched
    .filter(F.col("quantity_cume_dist").between(0.25, 0.45))
    .withColumn("quantity_cume_dist_dense_rank", F.dense_rank().over(w3))
    .withColumn("quantity_dense_rank_cume_dist", F.cume_dist().over(w4))
)

joined = (
    with_windows
    .alias("a")
    .join(df_products.alias("p"), F.col("a.product_id") == F.col("p.id"), "left")
    .select("a.*", F.col("p.product_name"), F.col("p.stock_qty"))
)

base = (
    joined
    .filter(F.col("product_id").isNull())
    .withColumn("quantity_cume_dist_whole", F.floor("quantity_cume_dist"))
)

scored = (
    base
    .select(
        "product_id",
        "line_total",
        "quantity_cume_dist_whole",
        F.percent_rank().over(w5).alias("quantity_cume_dist_percent_rank"),
        F.dense_rank().over(w6).alias("line_total_dense_rank"),
        F.rank().over(w7).alias("quantity_cume_dist_rank"),
    )
)

result = (
    scored
    .join(unit_price_by_product_id, on="product_id", how="left")
    .select(
        "product_id",
        "line_total",
        "quantity_cume_dist_whole",
        "quantity_cume_dist_percent_rank",
        "line_total_dense_rank",
        "quantity_cume_dist_rank",
        F.col("sum_order_item_unit_price"),
    )
    .orderBy(F.col("quantity_cume_dist_rank").asc_nulls_last(), "product_id")
)
