from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.month("launch_date")).orderBy(F.col("stock_qty").asc_nulls_first())
w2 = Window.partitionBy("product_id", "status").orderBy("stock_qty", "id")
w3 = Window.orderBy("stock_qty", "id")
w4 = Window.partitionBy("status").orderBy("launch_date", "id")
w5 = Window.partitionBy("status").orderBy(F.col("event_seq").desc_nulls_last(), "id")
w6 = Window.orderBy("stock_qty_rank", "id")
w_recent = Window.partitionBy("status").orderBy(F.col("event_seq").desc(), "id")
w8 = Window.orderBy("next_stock_qty", "id")
w9 = Window.partitionBy("status").orderBy("stock_qty_rank", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

with_dims = (
    df_order_items
    .alias("e")
    .join(df_orders.alias("o"), F.col("e.order_id") == F.col("o.id"), "left")
    .select(
        "e.id",
        "e.order_id",
        "e.product_id",
        "e.quantity",
        F.col("o.amount"),
        F.col("o.status"),
        F.col("o.discount"),
    )
)

joined = (
    with_dims
    .alias("a")
    .join(df_products.alias("p"), F.col("a.product_id") == F.col("p.id"), "left")
    .filter(F.array_contains(F.col("p.tags"), "sale"))
    .select(
        "a.id",
        "a.product_id",
        "a.status",
        F.col("p.launch_date"),
        F.col("p.product_name"),
        F.col("p.stock_qty"),
    )
)

valid = (
    joined
    .filter(F.col("launch_date") <= "2023-12-01")
    .select(
        "*",
        F.when(F.col("stock_qty") >= 431, "High").when(F.col("stock_qty") >= 142, "Medium").otherwise("Low").alias("stock_qty_band"),
        F.coalesce("status", F.lit("DIRECT")).alias("resolved_status"),
    )
)

enriched = (
    valid
    .select(
        "id",
        "status",
        F.rank().over(w).alias("stock_qty_rank"),
        F.row_number().over(w2).alias("event_seq"),
        F.lag("stock_qty", 2).over(w3).alias("prev_stock_qty"),
        F.lead("stock_qty", 2).over(w4).alias("next_stock_qty"),
    )
)

deduped = (
    enriched
    .withColumn("rn", F.row_number().over(w5))
    .filter(F.col("rn") == 1)
)

enriched_order_items = (
    deduped
    .withColumn("stock_qty_rank_bucket", F.ntile(5).over(w6))
    .withColumn("seq_no", F.row_number().over(w_recent))
    .withColumn("next_stock_qty_bucket", F.ntile(10).over(w8))
    .withColumn("first_known_event_seq", F.first("event_seq", ignorenulls=True).over(w9))
)

result = (
    enriched_order_items
    .rollup("status", "seq_no")
    .agg(
        F.round(F.avg("first_known_event_seq"), 2).alias("avg_first_known_event_seq"),
        F.sort_array(F.collect_set("status")).alias("status_list"),
        F.sort_array(F.collect_set("status")).alias("status_list_2"),
        F.count(F.when(F.col("status") == "RETURNED", 1)).alias("returned_count"),
        F.percentile_approx("event_seq", 0.9).alias("event_seq_p90"),
        F.count("*").alias("event_count"),
        F.grouping("status").alias("grouping_level"),
    )
    .filter(F.col("avg_first_known_event_seq") > 0)
)
