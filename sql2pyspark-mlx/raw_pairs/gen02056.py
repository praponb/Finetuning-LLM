from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("monthly_fee_whole").desc())
w_part = Window.partitionBy("status").orderBy("monthly_fee", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_grp = Window.orderBy("monthly_fee_whole", "id")
w4 = Window.partitionBy("prev_last_known_monthly_fee_tier").orderBy(F.col("avg_monthly_fee_whole").desc())

base = (
    df_subscriptions
    .filter((F.col("end_date") >= F.to_date(F.lit("2023-04-24"))) & (F.col("customer_id") == 36))
    .select(
        "id",
        "monthly_fee",
        "status",
        F.when(F.col("plan").isin(["premium", "enterprise"]), "Core").otherwise("Other").alias("plan_group"),
        F.floor("monthly_fee").alias("monthly_fee_whole"),
    )
)

deduped = (
    base
    .withColumn("dedup_rank", F.rank().over(w))
    .filter(F.col("dedup_rank") == 1)
)

metrics = (
    deduped
    .select(
        "id",
        "monthly_fee",
        "plan_group",
        "monthly_fee_whole",
        "dedup_rank",
        F.sum("monthly_fee_whole").over(w_part).alias("running_sum_monthly_fee_whole"),
        F.min("monthly_fee_whole").over(w_part).alias("running_min_monthly_fee_whole"),
        F.avg("monthly_fee_whole").over(w_part).alias("running_avg_monthly_fee_whole"),
        F.last("monthly_fee", ignorenulls=True).over(w_part).alias("last_known_monthly_fee"),
    )
)

with_windows = (
    metrics
    .select(
        "id",
        "plan_group",
        "monthly_fee_whole",
        "running_sum_monthly_fee_whole",
        "last_known_monthly_fee",
        F.lag("running_avg_monthly_fee_whole").over(w_grp).alias("prev_running_avg_monthly_fee_whole"),
        F.lead("running_avg_monthly_fee_whole").over(w_grp).alias("next_running_avg_monthly_fee_whole"),
        F.lead("last_known_monthly_fee").over(w_grp).alias("next_last_known_monthly_fee"),
        F.lag("last_known_monthly_fee").over(w_grp).alias("prev_last_known_monthly_fee"),
        F.row_number().over(w_grp).alias("event_seq"),
    )
)

result = (
    with_windows
    .groupBy(
        F.when(F.col("prev_last_known_monthly_fee") >= 263, "LARGE").otherwise("SMALL").alias("prev_last_known_monthly_fee_tier"),
        F.when(F.col("last_known_monthly_fee") >= 231, "PREMIUM").otherwise("STANDARD").alias("last_known_monthly_fee_tier"),
        F.when(F.col("next_last_known_monthly_fee") >= 261, "PREMIUM").otherwise("STANDARD").alias("next_last_known_monthly_fee_tier"),
    )
    .agg(
        F.round(F.avg("monthly_fee_whole"), 2).alias("avg_monthly_fee_whole"),
        F.sum("last_known_monthly_fee").alias("total_last_known_monthly_fee"),
    )
    .withColumn("avg_monthly_fee_whole_rank", F.rank().over(w4))
)
