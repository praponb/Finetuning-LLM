from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city")
w_hist = Window.partitionBy("city", "segment").orderBy("signup_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("segment")
w4 = Window.orderBy("corr_credit_limit_age", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.orderBy("running_min_age", "id")

cleaned = (
    df_customers
    .filter((F.col("signup_date") >= F.lit("2023-03-14").cast("date")) & (F.col("credit_limit") <= 4400))
    .select(
        "id",
        "name",
        "email",
        "city",
        "segment",
        "signup_date",
        "credit_limit",
        "age",
        F.when(F.col("is_active"), "Yes").otherwise("No").alias("is_active_label"),
    )
)

with_windows = (
    cleaned
    .select(
        "id",
        "age",
        "is_active_label",
        F.corr("credit_limit", "age").over(w).alias("corr_credit_limit_age"),
        F.last("segment", ignorenulls=True).over(w_hist).alias("last_known_segment"),
        F.corr("credit_limit", "age").over(w3).alias("corr_credit_limit_age_2"),
    )
)

enriched = (
    with_windows
    .filter(F.col("corr_credit_limit_age_2").isNull())
    .withColumn("running_min_age", F.min("age").over(w4))
    .withColumn("running_sum_age", F.sum("age").over(w4))
    .withColumn("running_avg_age", F.avg("age").over(w4))
    .withColumn("running_count_age", F.count("age").over(w4))
)

with_windows_customers = (
    enriched
    .withColumn("next_corr_credit_limit_age", F.lead("corr_credit_limit_age").over(w5))
    .withColumn("prev_corr_credit_limit_age_2", F.lag("corr_credit_limit_age_2").over(w5))
    .withColumn("seq_no", F.row_number().over(w5))
)

kept = (
    with_windows_customers
    .alias("s")
    .join(df_orders.alias("x"), (F.col("x.customer_id") == F.col("s.id")) & (F.col("x.discount") < 0.28), "left_semi")
)

not_flagged = (
    kept
    .alias("s")
    .join(df_subscriptions.alias("ch"), (F.col("ch.customer_id") == F.col("s.id")) & (F.col("ch.plan").isin(["premium", "standard"])), "left_anti")
)

result = (
    not_flagged
    .withColumn("seq_no_tier", F.when(F.col("seq_no") >= 8, "LARGE").otherwise("SMALL"))
    .withColumn("corr_credit_limit_age_2_tier", F.when(F.col("corr_credit_limit_age_2") >= 0.37, "HIGH").otherwise("LOW"))
    .groupingSets(
        [["seq_no_tier", "corr_credit_limit_age_2_tier"], ["seq_no_tier"]],
        "seq_no_tier",
        "corr_credit_limit_age_2_tier",
    )
    .agg(
        F.percentile_approx("running_min_age", [0.25, 0.5, 0.75]).alias("running_min_age_quartiles"),
        F.stddev_samp("age").alias("age_stddev"),
        F.countDistinct("id").alias("distinct_ids"),
        F.round(F.avg("running_avg_age"), 2).alias("avg_running_avg_age"),
        F.percentile_approx("next_corr_credit_limit_age", [0.25, 0.5, 0.75]).alias("next_corr_credit_limit_age_quartiles"),
    )
)
