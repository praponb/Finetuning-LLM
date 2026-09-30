from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.partitionBy("promo_code").orderBy(F.col("discount_pct").desc())
w2 = Window.partitionBy(F.date_trunc("YEAR", "ends_on"), "promo_code")
w3 = Window.partitionBy("promo_code").orderBy("adjusted_discount_pct", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.orderBy("running_sum_discount_pct_dense_rank_times_adjusted_discount_pct", "id").rowsBetween(-6, Window.currentRow)
w5 = Window.orderBy("running_sum_discount_pct_dense_rank_times_adjusted_discount_pct", "id")
w6 = Window.orderBy("adjusted_discount_pct", "id")
w7 = Window.orderBy(F.col("adjusted_discount_pct_bucket").desc(), "id")
w_ordered = Window.orderBy("ends_on_period_sum_adjusted_discount_pct_plus_adjusted_discount_pct", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w9 = Window.orderBy("ends_on_period_sum_adjusted_discount_pct_plus_adjusted_discount_pct", "id").rowsBetween(-2, Window.currentRow)

cleaned = (
    df_promotions
    .filter(F.col("channel").isNotNull() & F.col("ends_on").between("2025-02-01", "2025-08-01"))
    .select(
        "*",
        F.round((F.col("discount_pct") * 0.9), 2).alias("adjusted_discount_pct"),
    )
)

with_windows = (
    cleaned
    .select(
        "id",
        "channel",
        "promo_code",
        "adjusted_discount_pct",
        F.dense_rank().over(w_part).alias("discount_pct_dense_rank"),
        F.sum("adjusted_discount_pct").over(w2).alias("ends_on_period_sum_adjusted_discount_pct"),
    )
)

base = (
    with_windows
    .filter(~F.col("channel").eqNullSafe(F.lit("web")))
    .select(
        "*",
        (F.col("ends_on_period_sum_adjusted_discount_pct") + F.col("adjusted_discount_pct")).alias("ends_on_period_sum_adjusted_discount_pct_plus_adjusted_discount_pct"),
    )
)

base_promotions = (
    base
    .filter(F.col("channel").isNotNull() & F.col("ends_on_period_sum_adjusted_discount_pct").between(144, 266))
    .select(
        "*",
        (F.col("discount_pct_dense_rank") * F.col("adjusted_discount_pct")).alias("discount_pct_dense_rank_times_adjusted_discount_pct"),
        F.coalesce("channel", F.lit("OTHER")).alias("resolved_channel"),
    )
)

with_windows_promotions = (
    base_promotions
    .select(
        "id",
        "adjusted_discount_pct",
        "discount_pct_dense_rank",
        "ends_on_period_sum_adjusted_discount_pct_plus_adjusted_discount_pct",
        "discount_pct_dense_rank_times_adjusted_discount_pct",
        "resolved_channel",
        F.sum("discount_pct_dense_rank_times_adjusted_discount_pct").over(w3).alias("running_sum_discount_pct_dense_rank_times_adjusted_discount_pct"),
        F.min("discount_pct_dense_rank_times_adjusted_discount_pct").over(w3).alias("running_min_discount_pct_dense_rank_times_adjusted_discount_pct"),
        F.max("discount_pct_dense_rank_times_adjusted_discount_pct").over(w3).alias("running_max_discount_pct_dense_rank_times_adjusted_discount_pct"),
    )
)

metrics = (
    with_windows_promotions
    .filter(F.col("discount_pct_dense_rank") >= 4)
    .withColumn("running_max_discount_pct_dense_rank_times_adjusted_discount_pct_avg_6", F.avg("running_max_discount_pct_dense_rank_times_adjusted_discount_pct").over(w4))
    .withColumn("running_sum_discount_pct_dense_rank_times_adjusted_discount_pct_bucket", F.ntile(10).over(w5))
    .withColumn("adjusted_discount_pct_bucket", F.ntile(3).over(w6))
)

result = (
    metrics
    .select(
        "id",
        "adjusted_discount_pct",
        "discount_pct_dense_rank",
        "discount_pct_dense_rank_times_adjusted_discount_pct",
        "running_min_discount_pct_dense_rank_times_adjusted_discount_pct",
        "running_max_discount_pct_dense_rank_times_adjusted_discount_pct",
        "adjusted_discount_pct_bucket",
        F.row_number().over(w7).alias("row_seq"),
        F.sum("discount_pct_dense_rank").over(w_ordered).alias("cumulative_discount_pct_dense_rank"),
        F.lead("adjusted_discount_pct_bucket", 1).over(w5).alias("next_adjusted_discount_pct_bucket"),
        F.avg("discount_pct_dense_rank_times_adjusted_discount_pct").over(w9).alias("discount_pct_dense_rank_times_adjusted_discount_pct_avg_2"),
        F.sum("adjusted_discount_pct_bucket").over(w_ordered).alias("running_adjusted_discount_pct_bucket"),
    )
)
