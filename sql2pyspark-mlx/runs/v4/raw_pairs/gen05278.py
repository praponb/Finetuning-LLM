from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("is_standard", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("status").orderBy("end_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("status").orderBy("end_date", "id").rowsBetween(-6, -1)
w4 = Window.partitionBy("status").orderBy("running_monthly_fee", "id")
w5 = Window.orderBy("running_monthly_fee", "id")
w6 = Window.partitionBy("plan").orderBy("is_standard", "id")
w7 = Window.partitionBy("status").orderBy(F.col("end_date").desc(), "id")
w8 = Window.partitionBy("plan").orderBy(F.col("monthly_fee").asc_nulls_first(), "id")
w9 = Window.partitionBy("plan").orderBy(F.col("cumulative_is_standard").desc_nulls_last())
w10 = Window.orderBy(F.col("next_running_monthly_fee").asc_nulls_first())

cleaned = (
    df_subscriptions
    .filter(F.col("start_date") > F.lit("2023-12-16").cast("date"))
    .select(
        "id",
        "customer_id",
        "plan",
        "end_date",
        "monthly_fee",
        "status",
        F.when(F.col("plan") == "standard", 1).otherwise(0).alias("is_standard"),
    )
)

with_windows = (
    cleaned
    .withColumn("cumulative_is_standard", F.sum("is_standard").over(w))
    .withColumn("running_monthly_fee", F.sum("monthly_fee").over(w2))
)

scored = (
    with_windows
    .select(
        "*",
        F.avg("cumulative_is_standard").over(w3).alias("cumulative_is_standard_avg_6_prior"),
        F.lead("monthly_fee", 2, 0).over(w4).alias("next_monthly_fee"),
        F.lag("monthly_fee", 1, 0).over(w5).alias("prev_monthly_fee"),
        F.lead("running_monthly_fee", 1).over(w6).alias("next_running_monthly_fee"),
        F.row_number().over(w7).alias("event_seq"),
    )
)

deduped = (
    scored
    .withColumn("dedup_rank", F.row_number().over(w8))
    .filter(F.col("dedup_rank") == 1)
)

kept_keys = (
    deduped
    .select("customer_id", "id")
    .subtract(
        df_subscriptions
        .filter(F.col("customer_id").isin([11, 13, 16, 37]))
        .select("customer_id", "id")
    )
)

kept_rows = (
    deduped
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.customer_id") == F.col("k.customer_id")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

with_windows_subscriptions = (
    kept_rows
    .select(
        "*",
        F.rank().over(w9).alias("cumulative_is_standard_rank"),
        F.cume_dist().over(w10).alias("next_running_monthly_fee_cume_dist"),
    )
)

result = (
    with_windows_subscriptions
    .select(
        "plan",
        F.col("next_monthly_fee").alias("NEXT_MONTHLY_FEE_VAL"),
        F.col("prev_monthly_fee").alias("PREV_MONTHLY_FEE_VAL"),
    )
    .unpivot(
        ["plan"],
        ["NEXT_MONTHLY_FEE_VAL", "PREV_MONTHLY_FEE_VAL"],
        "metric",
        "value",
    )
)
