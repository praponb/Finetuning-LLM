from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan").orderBy("monthly_fee", "id")
w2 = Window.partitionBy(F.year("start_date"), "plan").orderBy("monthly_fee", "id").rowsBetween(-2, -1)
w3 = Window.partitionBy(F.year("start_date"), "status")
w4 = Window.partitionBy("plan", "status").orderBy(F.col("monthly_fee_plus_monthly_fee_share").asc_nulls_first())
w5 = Window.partitionBy("status").orderBy(F.col("monthly_fee_avg_2_prior").desc_nulls_last())
w6 = Window.partitionBy("status").orderBy(F.col("monthly_fee_plus_monthly_fee_share").asc_nulls_first(), "id")
w7 = Window.partitionBy("plan", "status").orderBy("dedup_rank", "id")

filtered = (
    df_subscriptions
    .filter((F.col("start_date") > F.to_date(F.lit("2023-03-15"))) & F.col("customer_id").isNull())
    .select(
        "id",
        "plan",
        "start_date",
        "monthly_fee",
        "status",
        F.concat_ws("-", "status", "plan").alias("status_plan_key"),
    )
)

metrics = (
    filtered
    .withColumn("event_seq", F.row_number().over(w))
    .withColumn("monthly_fee_avg_2_prior", F.avg("monthly_fee").over(w2))
    .withColumn("monthly_fee_share", F.round((F.col("monthly_fee") / F.sum("monthly_fee").over(w3)), 4))
)

valid = (
    metrics
    .filter((F.col("start_date") > F.lit("2023-03-26").cast("date")) & (F.col("plan") != "basic"))
    .select(
        "id",
        "plan",
        "start_date",
        "status",
        "status_plan_key",
        "monthly_fee_avg_2_prior",
        "monthly_fee_share",
        (F.col("monthly_fee") + F.col("monthly_fee_share")).alias("monthly_fee_plus_monthly_fee_share"),
    )
)

metrics_subscriptions = (
    valid
    .select(
        "id",
        "plan",
        "start_date",
        "status",
        "status_plan_key",
        "monthly_fee_avg_2_prior",
        "monthly_fee_plus_monthly_fee_share",
        F.dense_rank().over(w4).alias("monthly_fee_plus_monthly_fee_share_dense_rank"),
        F.percent_rank().over(w5).alias("monthly_fee_avg_2_prior_percent_rank"),
    )
)

ranked = (
    metrics_subscriptions
    .withColumn("dedup_rank", F.row_number().over(w6))
    .filter(F.col("dedup_rank") == 1)
)

result = (
    ranked
    .select(
        "id",
        "plan",
        "start_date",
        "status",
        "status_plan_key",
        "monthly_fee_plus_monthly_fee_share",
        F.lead("monthly_fee_avg_2_prior_percent_rank").over(w7).alias("next_monthly_fee_avg_2_prior_percent_rank"),
        F.lag("monthly_fee_avg_2_prior_percent_rank").over(w7).alias("prev_monthly_fee_avg_2_prior_percent_rank"),
        F.lag("monthly_fee_plus_monthly_fee_share_dense_rank").over(w7).alias("prev_monthly_fee_plus_monthly_fee_share_dense_rank"),
        F.row_number().over(w7).alias("seq_no"),
    )
)
