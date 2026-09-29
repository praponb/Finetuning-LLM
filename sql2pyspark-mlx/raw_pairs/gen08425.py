from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.partitionBy("product_id").orderBy("quantity_whole", "id")
w2 = Window.orderBy("quantity_whole", "id")
w3 = Window.partitionBy(F.year("last_restock")).orderBy(F.col("prev_quantity_whole").desc_nulls_last())
w4 = Window.orderBy(F.col("prev_quantity_whole").desc(), "id")
w5 = Window.orderBy(F.col("quantity_whole").desc())
w6 = Window.orderBy("event_seq_rounded", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_inventory
    .filter(F.col("store_id").isin([1, 2, 3, 4]))
    .select(
        "id",
        "product_id",
        "last_restock",
        F.ceil("quantity").alias("quantity_whole"),
        F.round("quantity", 2).alias("quantity_rounded"),
    )
)

metrics = (
    filtered
    .select(
        "id",
        "last_restock",
        "quantity_whole",
        "quantity_rounded",
        F.lag("quantity_rounded").over(w_recent).alias("prev_quantity_rounded"),
        F.lag("quantity_whole").over(w_recent).alias("prev_quantity_whole"),
        F.lead("quantity_whole").over(w_recent).alias("next_quantity_whole"),
        F.row_number().over(w_recent).alias("event_seq"),
    )
)

with_windows = (
    metrics
    .select(
        "id",
        "quantity_whole",
        "prev_quantity_rounded",
        "prev_quantity_whole",
        "event_seq",
        F.lag("prev_quantity_whole").over(w2).alias("prev_prev_quantity_whole"),
        F.lead("prev_quantity_whole").over(w2).alias("next_prev_quantity_whole"),
        F.lead("next_quantity_whole").over(w2).alias("next_next_quantity_whole"),
        F.row_number().over(w2).alias("seq_no"),
        F.percent_rank().over(w3).alias("prev_quantity_whole_percent_rank"),
    )
)

scored = (
    with_windows
    .withColumn("prev_prev_quantity_whole_percent_rank", F.lag("prev_quantity_whole_percent_rank", 1).over(w2))
    .withColumn("seq_no_2", F.row_number().over(w4))
    .withColumn("quantity_whole_dense_rank", F.dense_rank().over(w5))
)

filtered_inventory = (
    scored
    .filter(F.col("seq_no_2").between(5, 8))
    .select(
        "id",
        "quantity_whole",
        "prev_quantity_rounded",
        "prev_quantity_whole",
        "prev_prev_quantity_whole",
        "next_prev_quantity_whole",
        "next_next_quantity_whole",
        "seq_no",
        "prev_quantity_whole_percent_rank",
        "seq_no_2",
        "quantity_whole_dense_rank",
        F.round("event_seq", 2).alias("event_seq_rounded"),
        F.round("prev_quantity_whole_percent_rank", 1).alias("prev_quantity_whole_percent_rank_rounded"),
    )
)

enriched = (
    filtered_inventory
    .withColumn("running_avg_prev_quantity_whole_percent_rank", F.avg("prev_quantity_whole_percent_rank").over(w6))
    .withColumn("running_sum_prev_quantity_whole_percent_rank", F.sum("prev_quantity_whole_percent_rank").over(w6))
    .withColumn("running_max_prev_quantity_whole_percent_rank", F.max("prev_quantity_whole_percent_rank").over(w6))
    .withColumn("running_min_prev_quantity_whole_percent_rank", F.min("prev_quantity_whole_percent_rank").over(w6))
)

cleaned = (
    enriched
    .filter(F.col("prev_quantity_whole_percent_rank_rounded").between(40, 51))
    .withColumn("next_next_quantity_whole_times_prev_prev_quantity_whole", (F.col("next_next_quantity_whole") * F.col("prev_prev_quantity_whole")))
)

grouped = (
    cleaned
    .groupBy("seq_no")
    .agg(
        F.count("*").alias("row_count"),
        F.countDistinct("id").alias("distinct_ids"),
        F.max("quantity_whole").alias("max_quantity_whole"),
        F.avg("quantity_whole").alias("avg_quantity_whole"),
        F.percentile_approx("next_prev_quantity_whole", [0.25, 0.5, 0.75]).alias("next_prev_quantity_whole_quartiles"),
    )
    .filter(F.col("row_count") > 1)
)

result = (
    grouped
    .select(
        "seq_no",
        "distinct_ids",
        "max_quantity_whole",
        "avg_quantity_whole",
        "next_prev_quantity_whole_quartiles",
    )
)
