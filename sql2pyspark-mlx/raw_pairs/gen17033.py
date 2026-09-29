from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel").orderBy("discount_pct", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_grp = Window.partitionBy("channel").orderBy("running_min_discount_pct", "id")
w3 = Window.partitionBy("channel").orderBy(F.col("running_avg_discount_pct").desc(), "id")
w4 = Window.partitionBy(F.month("starts_on"))
w5 = Window.partitionBy("channel").orderBy(F.col("running_avg_discount_pct").desc_nulls_last())

enriched = (
    df_promotions
    .select(
        "id",
        "channel",
        "starts_on",
        "ends_on",
        "discount_pct",
        F.avg("discount_pct").over(w).alias("running_avg_discount_pct"),
        F.count("discount_pct").over(w).alias("running_count_discount_pct"),
        F.min("discount_pct").over(w).alias("running_min_discount_pct"),
    )
)

with_windows = (
    enriched
    .select(
        "id",
        "channel",
        "starts_on",
        "discount_pct",
        "running_avg_discount_pct",
        "running_count_discount_pct",
        F.lag("running_count_discount_pct").over(w_grp).alias("prev_running_count_discount_pct"),
        F.lead("running_min_discount_pct").over(w_grp).alias("next_running_min_discount_pct"),
        F.lag("running_min_discount_pct").over(w_grp).alias("prev_running_min_discount_pct"),
        F.row_number().over(w_grp).alias("event_seq"),
    )
)

ranked = (
    with_windows
    .withColumn("rank_in_group", F.row_number().over(w3))
    .filter(F.col("rank_in_group") == 1)
    .drop("rank_in_group")
)

metrics = (
    ranked
    .withColumn("prev_running_count_discount_pct_share", F.round((F.col("prev_running_count_discount_pct") / F.sum("prev_running_count_discount_pct").over(w4)), 4))
    .withColumn("running_avg_discount_pct_rank", F.rank().over(w5))
)

result = (
    metrics
    .select(
        "id",
        "channel",
        F.col("prev_running_count_discount_pct_share").alias("PREV_RUNNING_COUNT_DISCOUNT_PCT_SHARE_VAL"),
        F.col("discount_pct").alias("DISCOUNT_PCT_VAL"),
        F.col("next_running_min_discount_pct").alias("NEXT_RUNNING_MIN_DISCOUNT_PCT"),
        F.col("prev_running_min_discount_pct").alias("PREV_RUNNING_MIN_DISCOUNT_PCT_VAL"),
    )
    .unpivot(
        ids=["id", "channel"],
        values=["PREV_RUNNING_COUNT_DISCOUNT_PCT_SHARE_VAL", "DISCOUNT_PCT_VAL", "NEXT_RUNNING_MIN_DISCOUNT_PCT", "PREV_RUNNING_MIN_DISCOUNT_PCT_VAL"],
        variableColumnName="measure",
        valueColumnName="value",
    )
    .filter(F.col("value").isNotNull())
)
