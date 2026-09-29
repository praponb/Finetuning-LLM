from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy(F.col("unit_price").desc_nulls_last())
w3 = Window.orderBy(F.col("quantity").desc(), "id")
w4 = Window.partitionBy("order_id", "product_id")
w5 = Window.partitionBy("order_id")
w6 = Window.partitionBy("order_id").orderBy("cumulative_unit_price_plus_unit_price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy("order_id").orderBy(F.col("quantity_share").desc_nulls_last())
w_ordered = Window.partitionBy("order_id").orderBy("quantity", "id")
w9 = Window.partitionBy("order_id").orderBy(F.col("quantity_share_plus_cumulative_unit_price_plus_unit_price").desc(), "id")

scored = (
    df_order_items
    .select(
        "id",
        "order_id",
        "product_id",
        "quantity",
        "unit_price",
        F.sum("unit_price").over(w).alias("cumulative_unit_price"),
        F.cume_dist().over(w2).alias("unit_price_cume_dist"),
        F.ntile(3).over(w3).alias("quantity_bucket"),
        F.round((F.col("quantity") / F.sum("quantity").over(w4)), 4).alias("quantity_share"),
    )
)

valid = (
    scored
    .filter(F.col("order_id") == 36)
    .select(
        "id",
        "order_id",
        "quantity",
        "unit_price",
        "cumulative_unit_price",
        "unit_price_cume_dist",
        "quantity_share",
        (F.col("cumulative_unit_price") + F.col("unit_price")).alias("cumulative_unit_price_plus_unit_price"),
    )
)

cleaned = (
    valid
    .filter(F.col("quantity_share") <= 0.48)
    .withColumn("quantity_share_plus_cumulative_unit_price_plus_unit_price", (F.col("quantity_share") + F.col("cumulative_unit_price_plus_unit_price")))
)

enriched = (
    cleaned
    .withColumn("unit_price_share", F.round((F.col("unit_price") / F.sum("unit_price").over(w5)), 4))
    .withColumn("running_quantity_share", F.sum("quantity_share").over(w6))
    .withColumn("quantity_share_rank", F.rank().over(w7))
)

enriched_order_items = (
    enriched
    .select(
        "*",
        F.lag("quantity_share_rank").over(w_ordered).alias("prev_quantity_share_rank"),
        F.lead("quantity_share_rank").over(w_ordered).alias("next_quantity_share_rank"),
        F.lead("running_quantity_share").over(w_ordered).alias("next_running_quantity_share"),
        F.row_number().over(w_ordered).alias("row_seq"),
        F.row_number().over(w9).alias("seq_no"),
    )
)

metrics = (
    enriched_order_items
    .withColumn("running_min_seq_no", F.min("seq_no").over(w6))
    .withColumn("running_count_seq_no", F.count("seq_no").over(w6))
    .withColumn("running_sum_seq_no", F.sum("seq_no").over(w6))
    .withColumn("running_max_seq_no", F.max("seq_no").over(w6))
)

result = (
    metrics
    .groupBy(F.when(F.col("seq_no") >= 8, "HIGH").otherwise("LOW").alias("seq_no_tier"))
    .agg(
        F.count("*").alias("event_count"),
        F.percentile_approx("cumulative_unit_price_plus_unit_price", 0.95).alias("cumulative_unit_price_plus_unit_price_p95"),
        F.round(F.avg("quantity_share"), 2).alias("avg_quantity_share"),
    )
)
