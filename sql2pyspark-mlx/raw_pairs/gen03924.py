from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy("revenue_whole")
w2 = Window.orderBy("revenue_whole", "id")
w_hist = Window.partitionBy("store_id").orderBy("revenue", "id")
w_hist_2 = Window.orderBy("revenue", "id")
w_grp = Window.partitionBy("store_id").orderBy("revenue_whole", "id")
w6 = Window.orderBy("prev_revenue_whole_2", "id")
w7 = Window.orderBy("revenue_bucket", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.orderBy("event_seq", "id")
w9 = Window.orderBy("next_event_seq", "id")
w10 = Window.orderBy("next_revenue_2", "id")

filtered = (
    df_store_sales
    .filter(F.col("product_id").isin([2, 9]))
    .select(
        "id",
        "store_id",
        "product_id",
        "units",
        "revenue",
        F.ceil("revenue").alias("revenue_whole"),
    )
)

deduped = (
    filtered
    .withColumn("row_num", F.rank().over(w))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

base = (
    deduped
    .filter(F.col("product_id") == 4)
    .select(
        "id",
        "store_id",
        "revenue",
        "revenue_whole",
        F.col("revenue_whole").cast("string").alias("revenue_whole_num"),
    )
)

enriched = (
    base
    .filter(F.col("revenue") < 3400)
    .select(
        "id",
        "revenue",
        F.row_number().over(w2).alias("event_seq"),
        F.lead("revenue", 1, 0).over(w_hist).alias("next_revenue"),
        F.ntile(10).over(w_hist_2).alias("revenue_bucket"),
        F.lag("revenue_whole", 1).over(w_grp).alias("prev_revenue_whole"),
        F.lag("revenue_whole", 2).over(w_grp).alias("prev_revenue_whole_2"),
    )
)

metrics = (
    enriched
    .select(
        "id",
        "event_seq",
        "next_revenue",
        "prev_revenue_whole",
        "prev_revenue_whole_2",
        F.lead("event_seq").over(w6).alias("next_event_seq"),
        F.lag("event_seq").over(w6).alias("prev_event_seq"),
        F.lead("revenue").over(w6).alias("next_revenue_2"),
        F.row_number().over(w6).alias("event_seq_2"),
        F.last("prev_revenue_whole", ignorenulls=True).over(w7).alias("last_known_prev_revenue_whole"),
    )
)

scored = (
    metrics
    .select(
        "*",
        F.lead("event_seq_2", 1, 0).over(w8).alias("next_event_seq_2"),
        F.row_number().over(w9).alias("seq_no"),
        F.ntile(5).over(w10).alias("next_revenue_2_bucket"),
    )
)

base_store_sales = (
    scored
    .filter(F.col("prev_revenue_whole") < 58)
    .withColumn("next_revenue_2_minus_seq_no", (F.col("next_revenue_2") - F.col("seq_no")))
)

summary = (
    base_store_sales
    .groupBy(
        F.when(F.col("prev_revenue_whole_2") >= 90, "HIGH").otherwise("LOW").alias("prev_revenue_whole_2_tier"),
        "seq_no",
    )
    .agg(
        F.percentile_approx("last_known_prev_revenue_whole", [0.25, 0.5, 0.75]).alias("last_known_prev_revenue_whole_quartiles"),
        F.countDistinct("id").alias("distinct_ids"),
        F.percentile_approx("last_known_prev_revenue_whole", 0.5).alias("last_known_prev_revenue_whole_p50"),
        F.min("next_revenue_2").alias("min_next_revenue_2"),
        F.percentile_approx("prev_revenue_whole", 0.5).alias("prev_revenue_whole_p50"),
        F.percentile_approx("prev_revenue_whole", 0.95).alias("prev_revenue_whole_p95"),
    )
)

result = (
    summary
    .select(
        "prev_revenue_whole_2_tier",
        "seq_no",
        "last_known_prev_revenue_whole_quartiles",
        "distinct_ids",
        "last_known_prev_revenue_whole_p50",
        "prev_revenue_whole_p50",
        "prev_revenue_whole_p95",
    )
    .orderBy(F.col("seq_no").desc())
)
