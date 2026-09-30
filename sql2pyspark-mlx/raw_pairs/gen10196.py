from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("line_total").desc())
w2 = Window.orderBy("line_total", "id")
w3 = Window.orderBy(F.col("line_total").desc_nulls_first())
w4 = Window.orderBy("line_total_bucket", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.orderBy(F.col("line_total_dense_rank").asc_nulls_last())
w6 = Window.orderBy("line_total_dense_rank")

filtered = (
    df_order_items
    .filter(F.col("product_id").isNotNull())
    .select(
        "id",
        "quantity",
        (F.col("quantity") * F.col("unit_price")).alias("line_total"),
    )
)

metrics = (
    filtered
    .select(
        "id",
        "line_total",
        F.dense_rank().over(w).alias("line_total_dense_rank"),
        F.ntile(5).over(w2).alias("line_total_bucket"),
        F.percent_rank().over(w3).alias("line_total_percent_rank"),
    )
)

metrics_order_items = (
    metrics
    .select(
        "id",
        "line_total_dense_rank",
        "line_total_bucket",
        F.sum("line_total_dense_rank").over(w4).alias("running_line_total_dense_rank"),
        F.rank().over(w5).alias("line_total_dense_rank_rank"),
        F.dense_rank().over(w).alias("line_total_dense_rank_2"),
        F.lag("line_total_percent_rank", 1, 0).over(w2).alias("prev_line_total_percent_rank"),
        F.percent_rank().over(w6).alias("line_total_dense_rank_percent_rank"),
    )
)

base = (
    metrics_order_items
    .filter(F.col("running_line_total_dense_rank").between(18, 38))
    .select(
        "id",
        "line_total_dense_rank",
        "line_total_bucket",
        "line_total_dense_rank_rank",
        "line_total_dense_rank_2",
        "prev_line_total_percent_rank",
        (F.col("prev_line_total_percent_rank") + F.col("line_total_dense_rank_2")).alias("prev_line_total_percent_rank_plus_line_total_dense_rank_2"),
    )
)

summary = (
    base
    .groupBy(
        F.when(F.col("line_total_dense_rank_rank") >= 9, "LARGE").otherwise("SMALL").alias("line_total_dense_rank_rank_tier"),
    )
    .agg(
        F.percentile_approx("line_total_bucket", [0.25, 0.5, 0.75]).alias("line_total_bucket_quartiles"),
        F.percentile_approx("line_total_bucket", [0.25, 0.5, 0.75]).alias("line_total_bucket_quartiles_2"),
        F.min("line_total_dense_rank_rank").alias("min_line_total_dense_rank_rank"),
    )
)

result = (
    summary
    .select(
        "line_total_dense_rank_rank_tier",
        "line_total_bucket_quartiles",
        "line_total_bucket_quartiles_2",
    )
    .orderBy(F.col("line_total_dense_rank_rank_tier").desc_nulls_first())
)
