from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan").orderBy("end_date", "id").rowsBetween(-2, Window.currentRow)
w2 = Window.partitionBy("customer_id", "status")
w3 = Window.partitionBy("plan").orderBy("customer_id_sum_monthly_fee", "id")
w4 = Window.orderBy(F.col("monthly_fee_plus_last3_avg_monthly_fee").desc(), "id")
w5 = Window.orderBy("last3_count_monthly_fee", "id").rowsBetween(-3, -1)
w6 = Window.orderBy("monthly_fee", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.orderBy("monthly_fee_avg_3_prior", "id")
w8 = Window.orderBy(F.col("row_seq").desc())
w9 = Window.orderBy("last3_avg_monthly_fee_rounded", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_hist = Window.orderBy("first_known_last3_avg_monthly_fee_rounded", "id").rowsBetween(-3, Window.currentRow)
w11 = Window.orderBy("monthly_fee_plus_last3_avg_monthly_fee", "id").rowsBetween(-3, -1)

scored = (
    df_subscriptions
    .select(
        "id",
        "plan",
        "monthly_fee",
        F.count("monthly_fee").over(w).alias("last3_count_monthly_fee"),
        F.sum("monthly_fee").over(w).alias("last3_sum_monthly_fee"),
        F.avg("monthly_fee").over(w).alias("last3_avg_monthly_fee"),
        F.max("monthly_fee").over(w).alias("last3_max_monthly_fee"),
        F.sum("monthly_fee").over(w2).alias("customer_id_sum_monthly_fee"),
    )
)

ranked = (
    scored
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
    .select("id", "monthly_fee", "last3_count_monthly_fee", "last3_avg_monthly_fee")
)

base = (
    ranked
    .filter(F.col("monthly_fee").between(90, 150))
    .withColumn("last3_avg_monthly_fee_rounded", F.round("last3_avg_monthly_fee", 0))
    .withColumn("monthly_fee_plus_last3_avg_monthly_fee", (F.col("monthly_fee") + F.col("last3_avg_monthly_fee")))
)

enriched = (
    base
    .select(
        "*",
        F.row_number().over(w4).alias("row_seq"),
        F.row_number().over(w4).alias("row_seq_2"),
        F.avg("monthly_fee").over(w5).alias("monthly_fee_avg_3_prior"),
        F.first("last3_avg_monthly_fee_rounded", ignorenulls=True).over(w6).alias("first_known_last3_avg_monthly_fee_rounded"),
    )
)

enriched_subscriptions = (
    enriched
    .filter(F.col("monthly_fee_plus_last3_avg_monthly_fee").between(22, 24))
    .select(
        "id",
        "monthly_fee",
        "last3_avg_monthly_fee",
        "last3_avg_monthly_fee_rounded",
        "monthly_fee_plus_last3_avg_monthly_fee",
        "row_seq",
        "monthly_fee_avg_3_prior",
        "first_known_last3_avg_monthly_fee_rounded",
        F.min("last3_count_monthly_fee").over(w6).alias("running_min_last3_count_monthly_fee"),
        F.max("last3_count_monthly_fee").over(w6).alias("running_max_last3_count_monthly_fee"),
        F.count("last3_count_monthly_fee").over(w6).alias("running_count_last3_count_monthly_fee"),
    )
)

scored_subscriptions = (
    enriched_subscriptions
    .withColumn("seq_no", F.row_number().over(w7))
    .withColumn("last_known_running_max_last3_count_monthly_fee", F.last("running_max_last3_count_monthly_fee", ignorenulls=True).over(w6))
    .withColumn("row_seq_percent_rank", F.percent_rank().over(w8))
    .withColumn("first_known_row_seq", F.first("row_seq", ignorenulls=True).over(w9))
)

metrics = (
    scored_subscriptions
    .select(
        "*",
        F.min("monthly_fee_avg_3_prior").over(w_hist).alias("last4_min_monthly_fee_avg_3_prior"),
        F.max("monthly_fee_avg_3_prior").over(w_hist).alias("last4_max_monthly_fee_avg_3_prior"),
        F.avg("monthly_fee_avg_3_prior").over(w_hist).alias("last4_avg_monthly_fee_avg_3_prior"),
        F.sum("monthly_fee_avg_3_prior").over(w_hist).alias("last4_sum_monthly_fee_avg_3_prior"),
        F.avg("monthly_fee").over(w11).alias("monthly_fee_avg_3_prior_2"),
    )
)

result = (
    metrics
    .select(
        "id",
        "last3_avg_monthly_fee",
        "monthly_fee_plus_last3_avg_monthly_fee",
        "first_known_last3_avg_monthly_fee_rounded",
    )
    .unpivot(
        ids=["id"],
        values=["last3_avg_monthly_fee", "monthly_fee_plus_last3_avg_monthly_fee", "first_known_last3_avg_monthly_fee_rounded"],
        variableColumnName="kpi",
        valueColumnName="value",
    )
)
