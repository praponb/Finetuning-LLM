from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("quantity_filled").desc(), "id")
w2 = Window.partitionBy(F.month("last_restock"), "store_id").orderBy(F.col("quantity_filled").asc_nulls_last())
w3 = Window.orderBy("quantity", "id")
w4 = Window.orderBy("quantity_filled", "id")
w5 = Window.orderBy("quantity_filled_bucket_scaled", "id")
w6 = Window.orderBy("event_seq", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_running = Window.orderBy("seq_no", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_inventory
    .filter(F.col("quantity") <= 227)
    .withColumn("quantity_filled", F.coalesce("quantity", F.lit(0)))
)

with_windows = (
    valid
    .select(
        "id",
        "quantity",
        "quantity_filled",
        F.ntile(10).over(w).alias("quantity_filled_bucket"),
        F.dense_rank().over(w2).alias("quantity_filled_dense_rank"),
    )
)

enriched = (
    with_windows
    .filter(F.col("quantity_filled_dense_rank") <= 3)
    .select(
        "id",
        "quantity",
        "quantity_filled",
        "quantity_filled_bucket",
        "quantity_filled_dense_rank",
        F.row_number().over(w3).alias("event_seq"),
        F.ntile(5).over(w4).alias("quantity_filled_bucket_2"),
    )
)

filtered = (
    enriched
    .filter(F.col("quantity_filled_bucket") >= 5)
    .select(
        "*",
        F.round((F.col("quantity_filled_bucket") * 1.15), 2).alias("quantity_filled_bucket_scaled"),
        (F.col("event_seq") - F.col("quantity_filled_bucket")).alias("event_seq_minus_quantity_filled_bucket"),
    )
)

scored = (
    filtered
    .select(
        "id",
        "quantity",
        "event_seq_minus_quantity_filled_bucket",
        F.row_number().over(w5).alias("seq_no"),
        F.first("quantity_filled_bucket_2", ignorenulls=True).over(w6).alias("first_known_quantity_filled_bucket_2"),
    )
)

scored_inventory = (
    scored
    .select(
        "*",
        F.avg("event_seq_minus_quantity_filled_bucket").over(w_running).alias("running_avg_event_seq_minus_quantity_filled_bucket"),
        F.max("event_seq_minus_quantity_filled_bucket").over(w_running).alias("running_max_event_seq_minus_quantity_filled_bucket"),
        F.count("event_seq_minus_quantity_filled_bucket").over(w_running).alias("running_count_event_seq_minus_quantity_filled_bucket"),
        F.row_number().over(w3).alias("seq_no_2"),
    )
)

result = (
    scored_inventory
    .groupBy("seq_no")
    .agg(
        F.count("*").alias("cnt"),
        F.percentile_approx("first_known_quantity_filled_bucket_2", 0.95).alias("first_known_quantity_filled_bucket_2_p95"),
        F.bit_or("quantity").alias("quantity_bit_or"),
        F.percentile_approx("running_avg_event_seq_minus_quantity_filled_bucket", [0.25, 0.5, 0.75]).alias("running_avg_event_seq_minus_quantity_filled_bucket_quartiles"),
        F.round(F.avg("seq_no"), 2).alias("avg_seq_no"),
        F.countDistinct("id").alias("distinct_ids"),
    )
    .orderBy(F.col("first_known_quantity_filled_bucket_2_p95").desc())
)
