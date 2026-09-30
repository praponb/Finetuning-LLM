from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id", "product_id").orderBy(F.col("unit_price").desc_nulls_last())
w2 = Window.orderBy("unit_price", "id")
w_part = Window.partitionBy("order_id").orderBy("unit_price", "id")
w_ordered = Window.partitionBy("order_id").orderBy("row_seq", "id")
w5 = Window.partitionBy("order_id", "product_id").orderBy("unit_price", "id")
w6 = Window.orderBy("prev_unit_price_cume_dist", "id")
w_hist = Window.orderBy(F.col("unit_price_bucket").desc(), "id")
w8 = Window.orderBy("next_quantity_whole", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w9 = Window.orderBy(F.col("running_max_unit_price").desc_nulls_last())
w10 = Window.orderBy("next_quantity_whole", "id")
w11 = Window.orderBy(F.col("running_sum_unit_price").desc(), "id")
w12 = Window.orderBy("seq_no_2", "id")
w13 = Window.orderBy(F.col("prev_unit_price_cume_dist").desc(), "id")

scored = (
    df_order_items
    .filter(F.col("product_id").isin([16, 23, 27, 39]))
    .select(
        "id",
        "order_id",
        "product_id",
        "unit_price",
        F.cume_dist().over(w).alias("unit_price_cume_dist"),
        F.row_number().over(w2).alias("row_seq"),
        F.lead("quantity", 1).over(w_part).alias("next_quantity"),
    )
)

with_windows = (
    scored
    .select(
        "id",
        "unit_price",
        "unit_price_cume_dist",
        "next_quantity",
        F.lead("next_quantity").over(w_ordered).alias("next_next_quantity"),
        F.lag("unit_price_cume_dist").over(w_ordered).alias("prev_unit_price_cume_dist"),
        F.row_number().over(w_ordered).alias("seq_no"),
        F.ntile(4).over(w5).alias("unit_price_bucket"),
    )
)

scored_order_items = (
    with_windows
    .withColumn("seq_no_2", F.row_number().over(w6))
    .withColumn("unit_price_bucket_bucket", F.ntile(10).over(w_hist))
)

base = (
    scored_order_items
    .filter(F.col("next_next_quantity") == 6)
    .select(
        "id",
        "unit_price",
        "unit_price_cume_dist",
        "next_next_quantity",
        "prev_unit_price_cume_dist",
        "seq_no",
        "seq_no_2",
        "unit_price_bucket_bucket",
        F.floor("next_quantity").alias("next_quantity_whole"),
    )
)

enriched = (
    base
    .select(
        "id",
        "unit_price",
        "unit_price_cume_dist",
        "prev_unit_price_cume_dist",
        "seq_no_2",
        "unit_price_bucket_bucket",
        "next_quantity_whole",
        F.avg("unit_price").over(w8).alias("running_avg_unit_price"),
        F.sum("unit_price").over(w8).alias("running_sum_unit_price"),
        F.count("unit_price").over(w8).alias("running_count_unit_price"),
        F.max("unit_price").over(w8).alias("running_max_unit_price"),
    )
)

enriched_order_items = (
    enriched
    .withColumn("running_max_unit_price_cume_dist", F.cume_dist().over(w9))
    .withColumn("next_seq_no_2", F.lead("seq_no_2", 1).over(w10))
    .withColumn("event_seq", F.row_number().over(w11))
    .withColumn("prev_unit_price", F.lag("unit_price", 2, 0).over(w12))
    .withColumn("prev_unit_price_cume_dist_bucket", F.ntile(3).over(w13))
)

cleaned = (
    enriched_order_items
    .filter(F.col("running_max_unit_price_cume_dist") <= 0.39)
    .withColumn("unit_price_minus_unit_price_cume_dist", (F.col("unit_price") - F.col("unit_price_cume_dist")))
)

result = (
    cleaned
    .groupBy(
        F.when(F.col("running_max_unit_price_cume_dist") >= 0.78, "HIGH").otherwise("LOW").alias("running_max_unit_price_cume_dist_tier"),
    )
    .agg(
        F.percentile_approx("running_max_unit_price_cume_dist", [0.25, 0.5, 0.75]).alias("running_max_unit_price_cume_dist_quartiles"),
        F.sum("unit_price_cume_dist").alias("total_unit_price_cume_dist"),
        F.count("*").alias("cnt"),
    )
    .orderBy(F.col("running_max_unit_price_cume_dist_tier").desc_nulls_last())
)
