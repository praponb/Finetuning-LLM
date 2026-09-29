from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.partitionBy("customer_id").orderBy("monthly_fee", "id")
w2 = Window.partitionBy("plan", "customer_id").orderBy("monthly_fee_rounded", "id").rowsBetween(-2, -1)
w_recent = Window.partitionBy("customer_id").orderBy("start_date_month", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy(F.year("end_date")).orderBy("weeks_start_date_to_end_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("plan").orderBy("start_date_month_avg_2_prior", "id")
w6 = Window.partitionBy("status").orderBy("start_date_month_avg_2_prior", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy("status").orderBy("monthly_fee_rounded")

base = (
    df_subscriptions
    .filter(F.year("end_date") == 2023)
    .select(
        "*",
        F.month("start_date").alias("start_date_month"),
        F.round("monthly_fee", 2).alias("monthly_fee_rounded"),
    )
)

with_windows = (
    base
    .filter(F.col("customer_id").isNotNull())
    .select(
        "*",
        F.lead("monthly_fee", 1).over(w_ordered).alias("next_monthly_fee"),
        F.avg("start_date_month").over(w2).alias("start_date_month_avg_2_prior"),
        F.last("monthly_fee_rounded", ignorenulls=True).over(w_recent).alias("last_known_monthly_fee_rounded"),
    )
)

valid = (
    with_windows
    .filter(F.col("end_date") >= F.lit("2023-10-27").cast("date"))
    .select(
        "*",
        (F.datediff("end_date", "start_date") / 7).alias("weeks_start_date_to_end_date"),
        (F.col("monthly_fee") / F.nullif("start_date_month_avg_2_prior", F.lit(0))).alias("monthly_fee_per_start_date_month_avg_2_prior"),
    )
)

scored = (
    valid
    .withColumn("first_known_monthly_fee", F.first("monthly_fee", ignorenulls=True).over(w4))
    .withColumn("event_seq", F.row_number().over(w5))
    .withColumn("last_known_weeks_start_date_to_end_date", F.last("weeks_start_date_to_end_date", ignorenulls=True).over(w6))
)

deduped = (
    scored
    .withColumn("dedup_rank", F.rank().over(w7))
    .filter(F.col("dedup_rank") == 1)
)

remaining_keys = (
    deduped
    .select("customer_id", "status")
    .exceptAll(
        df_subscriptions
        .filter(F.col("monthly_fee") <= 130)
        .select("customer_id", "status")
    )
)

verified = (
    deduped
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.customer_id") == F.col("k.customer_id")) & (F.col("s.status") == F.col("k.status")), "inner")
    .select("s.*")
)

valid_subscriptions = (
    verified
    .filter((F.col("start_date") > F.lit("2023-08-11").cast("date")) & F.col("customer_id").isNotNull())
    .select(
        "id",
        "customer_id",
        "plan",
        "start_date",
        "end_date",
        "status",
        "start_date_month",
        "monthly_fee_rounded",
        "next_monthly_fee",
        "start_date_month_avg_2_prior",
        "last_known_monthly_fee_rounded",
        "weeks_start_date_to_end_date",
        "monthly_fee_per_start_date_month_avg_2_prior",
        "event_seq",
        "last_known_weeks_start_date_to_end_date",
        "dedup_rank",
        F.trim("status").alias("clean_status"),
    )
)

result = (
    valid_subscriptions
    .groupBy("status", "plan")
    .agg(
        F.sum(F.when(F.col("status") == "EXPIRED", F.col("monthly_fee_rounded"))).alias("expired_total_monthly_fee_rounded"),
        F.round(F.sum("next_monthly_fee") / F.nullif(F.count(F.when(F.col("plan") == "premium", 1)), F.lit(0)), 2).alias("next_monthly_fee_per_premium"),
        F.size(F.collect_set("status")).alias("num_status"),
    )
    .filter(F.col("num_status") >= 1)
)
