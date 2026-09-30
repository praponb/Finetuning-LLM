from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy("quantity", "id")
w_ordered = Window.orderBy("next_quantity", "id")
w3 = Window.partitionBy("product_id", "order_id")
w4 = Window.partitionBy("product_id").orderBy("prev_unit_price", "id")
w5 = Window.partitionBy(F.year("order_date")).orderBy("unit_price", "id")
w6 = Window.orderBy("quantity", "id")
w7 = Window.partitionBy("order_id").orderBy(F.col("prev_prev_quantity").desc())
w8 = Window.partitionBy("order_id", "product_id")

joined = (
    df_order_items
    .alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "inner")
    .select("s.*", F.col("o.order_date"))
)

with_dims = (
    joined
    .alias("a")
    .join(df_products.alias("b"), F.col("a.product_id") == F.col("b.id"), "inner")
    .select("a.*", F.col("b.launch_date"), F.col("b.cost"), F.col("b.product_name"))
)

scored = (
    with_dims
    .select(
        "*",
        F.lag("unit_price").over(w).alias("prev_unit_price"),
        F.lead("unit_price").over(w).alias("next_unit_price"),
        F.lead("quantity").over(w).alias("next_quantity"),
        F.lag("quantity").over(w).alias("prev_quantity"),
        F.row_number().over(w).alias("row_seq"),
    )
)

with_windows = (
    scored
    .withColumn("row_seq_2", F.row_number().over(w_ordered))
    .withColumn("product_id_stddev_samp_next_quantity", F.stddev_samp("next_quantity").over(w3))
)

scored_order_items = (
    with_windows
    .select(
        "*",
        F.ntile(3).over(w4).alias("prev_unit_price_bucket"),
        F.lead("next_unit_price", 2, 0).over(w5).alias("next_next_unit_price"),
        F.lag("prev_quantity", 2).over(w6).alias("prev_prev_quantity"),
    )
)

scored_order_items_2 = (
    scored_order_items
    .filter(F.col("next_unit_price") >= 190)
    .withColumn("prev_prev_quantity_cume_dist", F.cume_dist().over(w7))
    .withColumn("prev_prev_quantity_share", F.round((F.col("prev_prev_quantity") / F.sum("prev_prev_quantity").over(w8)), 4))
)

daily = (
    scored_order_items_2
    .groupBy(
        F.when(F.col("cost") >= 560, "LARGE").otherwise("SMALL").alias("cost_tier"),
    )
    .agg(
        F.countDistinct("product_id").alias("distinct_product_ids"),
        F.percentile_approx("prev_unit_price", [0.25, 0.5, 0.75]).alias("prev_unit_price_quartiles"),
        F.bit_or("quantity").alias("quantity_bit_or"),
        F.percentile_approx("prev_prev_quantity_share", 0.9).alias("prev_prev_quantity_share_p90"),
    )
)

result = (
    daily
    .select("cost_tier", "prev_unit_price_quartiles", "prev_prev_quantity_share_p90")
)
