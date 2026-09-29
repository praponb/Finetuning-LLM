from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id")
w2 = Window.orderBy(F.col("unit_price").desc(), "id")
w3 = Window.partitionBy("order_id").orderBy("quantity", "id")
w4 = Window.partitionBy("order_id", "product_id").orderBy(F.col("unit_price").desc_nulls_first())
w5 = Window.partitionBy("order_id").orderBy(F.col("next_unit_price_bucket").asc_nulls_last())
w6 = Window.orderBy("unit_price_cume_dist", "id").rowsBetween(-3, Window.currentRow)
w7 = Window.partitionBy("product_id").orderBy("unit_price_cume_dist", "id")
w8 = Window.partitionBy("product_id").orderBy(F.col("next_unit_price_bucket_cume_dist").desc(), "id")
w9 = Window.partitionBy("order_id").orderBy(F.col("next_unit_price_bucket_cume_dist").desc(), "id")
w_recent = Window.partitionBy("order_id", "product_id").orderBy(F.col("next_unit_price_bucket").desc(), "id")
w11 = Window.partitionBy("order_id").orderBy(F.col("unit_price_cume_dist").desc(), "id")
w12 = Window.partitionBy("product_id").orderBy("next_unit_price_bucket_avg_3", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

enriched = (
    df_order_items
    .select(
        "*",
        F.corr("quantity", "unit_price").over(w).alias("corr_quantity_unit_price"),
        F.ntile(10).over(w2).alias("unit_price_bucket"),
    )
)

enriched_order_items = (
    enriched
    .select(
        "*",
        F.lead("unit_price_bucket", 2, 0).over(w3).alias("next_unit_price_bucket"),
        F.cume_dist().over(w4).alias("unit_price_cume_dist"),
    )
)

with_windows = (
    enriched_order_items
    .withColumn("next_unit_price_bucket_cume_dist", F.cume_dist().over(w5))
    .withColumn("next_unit_price_bucket_avg_3", F.avg("next_unit_price_bucket").over(w6))
    .withColumn("next_quantity", F.lead("quantity", 1, 0).over(w7))
)

base = (
    with_windows
    .filter(F.col("product_id").isNotNull() & (F.col("next_unit_price_bucket") == 7))
    .select(
        "*",
        (F.col("quantity") * F.col("next_unit_price_bucket_cume_dist")).alias("quantity_times_next_unit_price_bucket_cume_dist"),
    )
)

metrics = (
    base
    .select(
        "*",
        F.ntile(4).over(w8).alias("next_unit_price_bucket_cume_dist_bucket"),
        F.ntile(5).over(w9).alias("next_unit_price_bucket_cume_dist_bucket_2"),
        F.row_number().over(w_recent).alias("event_seq"),
        F.row_number().over(w11).alias("seq_no"),
    )
)

with_windows_order_items = (
    metrics
    .select(
        "*",
        F.last("quantity_times_next_unit_price_bucket_cume_dist", ignorenulls=True).over(w12).alias("last_known_quantity_times_next_unit_price_bucket_cume_dist"),
        F.sum("next_unit_price_bucket_cume_dist").over(w12).alias("cumulative_next_unit_price_bucket_cume_dist"),
    )
)

result = (
    with_windows_order_items
    .select(
        "id",
        "product_id",
        "corr_quantity_unit_price",
        "next_unit_price_bucket_cume_dist",
        "last_known_quantity_times_next_unit_price_bucket_cume_dist",
    )
    .unpivot(
        ["id", "product_id"],
        ["corr_quantity_unit_price", "next_unit_price_bucket_cume_dist", "last_known_quantity_times_next_unit_price_bucket_cume_dist"],
        "metric_name",
        "val",
    )
    .filter(F.col("val").isNotNull())
)
