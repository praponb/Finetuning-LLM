from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.partitionBy("product_id", "order_id")
w_recent = Window.partitionBy("order_id", "product_id").orderBy(F.col("unit_price").desc())
w3 = Window.partitionBy("product_id")
w4 = Window.partitionBy("order_id")
w5 = Window.partitionBy("order_id").orderBy(F.col("unit_price").asc_nulls_first())
w6 = Window.orderBy(F.col("corr_unit_price_quantity").asc_nulls_first())
w_grp = Window.partitionBy("status", "order_id")
w8 = Window.partitionBy("product_id", "status")

kept_keys = (
    df_order_items
    .select("product_id", "order_id")
    .subtract(
        df_order_items
        .filter(F.col("order_id").isin([2, 14, 18, 27]))
        .select("product_id", "order_id")
    )
)

verified = (
    df_order_items
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.product_id") == F.col("k.product_id")) & (F.col("s.order_id") == F.col("k.order_id")), "inner")
    .select("s.*")
)

metrics = (
    verified
    .withColumn("product_id_min_quantity", F.min("quantity").over(w_part))
    .withColumn("unit_price_cume_dist", F.cume_dist().over(w_recent))
    .withColumn("corr_unit_price_quantity", F.corr("unit_price", "quantity").over(w3))
    .withColumn("quantity_share", F.round((F.col("quantity") / F.sum("quantity").over(w4)), 4))
    .withColumn("unit_price_cume_dist_2", F.cume_dist().over(w5))
)

with_windows = (
    metrics
    .select(
        "*",
        F.avg("quantity_share").over(w4).alias("order_id_avg_quantity_share"),
        F.sum("unit_price_cume_dist_2").over(w4).alias("order_id_sum_unit_price_cume_dist_2"),
        F.min("unit_price_cume_dist").over(w3).alias("product_id_min_unit_price_cume_dist"),
        F.max("product_id_min_quantity").over(w3).alias("product_id_max_product_id_min_quantity"),
        F.rank().over(w6).alias("corr_unit_price_quantity_rank"),
    )
)

enriched = (
    with_windows
    .alias("src")
    .join(df_orders.alias("b"), F.col("src.order_id") == F.col("b.id"), "left")
    .select("src.*", F.col("b.status"), F.col("b.order_date"))
)

enriched_order_items = (
    enriched
    .withColumn("status_sum_unit_price", F.sum("unit_price").over(w_grp))
    .withColumn("product_id_min_quantity_share", F.round((F.col("product_id_min_quantity") / F.sum("product_id_min_quantity").over(w8)), 4))
    .withColumn("product_id_max_product_id_min_quantity_2", F.max("product_id_min_quantity").over(w_part))
)

enriched_order_items_2 = (
    enriched_order_items
    .alias("e")
    .join(df_orders.alias("b"), F.col("e.order_id") == F.col("b.id"), "left")
    .select("e.*", F.col("b.channel"), F.col("b.discount"))
)

result = (
    enriched_order_items_2
    .withColumn("quarter_start", F.date_trunc("QUARTER", "order_date"))
    .cube("order_id", "quarter_start")
    .agg(
        F.size(F.collect_set("channel")).alias("num_channels"),
        F.count(F.when(F.col("channel") == "mobile", 1)).alias("mobile_rows"),
        F.sum(F.when(F.col("status") == "RETURNED", F.col("order_id_sum_unit_price_cume_dist_2"))).alias("returned_total_order_id_sum_unit_price_cume_dist_2"),
        F.sort_array(F.collect_set("status")).alias("status_list"),
        F.corr("quantity", "unit_price").alias("corr_quantity_unit_price"),
        F.sort_array(F.collect_set("channel")).alias("channel_list"),
        F.grouping_id().alias("grouping_level"),
    )
    .orderBy(F.col("quarter_start").asc_nulls_first())
)
