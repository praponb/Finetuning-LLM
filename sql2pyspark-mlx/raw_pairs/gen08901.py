from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.partitionBy("segment", "plan").orderBy("monthly_fee", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_part = Window.partitionBy("status", "segment").orderBy(F.col("monthly_fee").desc(), "id")
w3 = Window.partitionBy("customer_id", "segment").orderBy("running_sum_monthly_fee")
w4 = Window.partitionBy("plan").orderBy("monthly_fee_bucket", "id").rowsBetween(-2, Window.currentRow)
w5 = Window.partitionBy(F.month("signup_date"), "customer_id")
w6 = Window.partitionBy("segment").orderBy(F.col("running_sum_monthly_fee").desc(), "id")
w7 = Window.partitionBy("segment").orderBy("end_date", "id")

enriched = (
    df_subscriptions
    .alias("e")
    .join(df_customers.alias("b"), F.col("e.customer_id") == F.col("b.id"), "inner")
    .filter(F.col("b.age") < 54)
    .select("e.*", F.col("b.email"), F.col("b.segment"), F.col("b.signup_date"))
)

metrics = (
    enriched
    .select(
        "id",
        "customer_id",
        "plan",
        "end_date",
        "monthly_fee",
        "segment",
        "signup_date",
        F.avg("monthly_fee").over(w_ordered).alias("running_avg_monthly_fee"),
        F.max("monthly_fee").over(w_ordered).alias("running_max_monthly_fee"),
        F.sum("monthly_fee").over(w_ordered).alias("running_sum_monthly_fee"),
        F.ntile(3).over(w_part).alias("monthly_fee_bucket"),
    )
)

enriched_subscriptions = (
    metrics
    .select(
        "id",
        "customer_id",
        "segment",
        "signup_date",
        "running_avg_monthly_fee",
        "monthly_fee_bucket",
        F.cume_dist().over(w3).alias("running_sum_monthly_fee_cume_dist"),
        F.avg("monthly_fee_bucket").over(w4).alias("monthly_fee_bucket_avg_2"),
        F.round((F.col("running_sum_monthly_fee") / F.sum("running_sum_monthly_fee").over(w5)), 4).alias("running_sum_monthly_fee_share"),
        F.row_number().over(w6).alias("row_seq"),
        F.lag("monthly_fee", 1).over(w7).alias("prev_monthly_fee"),
    )
)

with_dims = (
    enriched_subscriptions
    .alias("e")
    .join(df_customers.alias("p"), F.col("e.customer_id") == F.col("p.id"), "left")
    .select(
        "e.*",
        F.col("p.email"),
        F.col("p.signup_date").alias("customer_signup_date"),
    )
)

result = (
    with_dims
    .rollup(
        "customer_id",
        F.when(F.col("monthly_fee_bucket") >= 7, "PREMIUM").otherwise("STANDARD").alias("monthly_fee_bucket_tier"),
    )
    .agg(
        F.percentile_approx("row_seq", 0.95).alias("row_seq_p95"),
        F.sum(F.when(F.col("segment") == "Consumer", F.col("running_sum_monthly_fee_share")).otherwise(0)).alias("consumer_running_sum_monthly_fee_share"),
        F.round(F.sum("monthly_fee_bucket") / F.nullif(F.count(F.when(F.col("segment") == "Corporate", 1)), F.lit(0)), 2).alias("monthly_fee_bucket_per_corporate"),
    )
    .orderBy(F.col("consumer_running_sum_monthly_fee_share").desc_nulls_first())
)
