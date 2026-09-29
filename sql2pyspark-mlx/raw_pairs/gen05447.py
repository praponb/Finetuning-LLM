from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("days_since_end_date", "id").rowsBetween(-6, Window.currentRow)
w2 = Window.partitionBy("plan").orderBy(F.col("last7_max_days_since_end_date").desc(), "id")
w3 = Window.partitionBy("country").orderBy("age", "id")
w4 = Window.partitionBy("plan").orderBy("dedup_rank", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("plan").orderBy("dedup_rank", "id")

filtered = (
    df_subscriptions
    .filter((F.col("start_date") > F.to_date(F.lit("2023-08-04"))) & F.col("customer_id").isNotNull())
    .withColumn("status_plan_key", F.concat_ws("-", "status", "plan"))
    .withColumn("days_since_end_date", F.datediff(F.current_date(), "end_date"))
)

enriched = (
    filtered
    .filter(F.col("customer_id").isin([2, 19, 25]))
    .select(
        "id",
        "customer_id",
        "plan",
        "monthly_fee",
        "days_since_end_date",
        F.avg("days_since_end_date").over(w).alias("last7_avg_days_since_end_date"),
        F.sum("days_since_end_date").over(w).alias("last7_sum_days_since_end_date"),
        F.min("days_since_end_date").over(w).alias("last7_min_days_since_end_date"),
        F.max("days_since_end_date").over(w).alias("last7_max_days_since_end_date"),
    )
)

ranked = (
    enriched
    .withColumn("dedup_rank", F.row_number().over(w2))
    .filter(F.col("dedup_rank") == 1)
)

joined = (
    ranked
    .alias("a")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "inner")
    .select(
        "a.id",
        "a.customer_id",
        "a.plan",
        "a.days_since_end_date",
        "a.last7_sum_days_since_end_date",
        "a.dedup_rank",
        F.col("c.age"),
    )
)

enriched_subscriptions = (
    joined
    .alias("s")
    .join(df_customers.alias("d"), F.col("s.customer_id").eqNullSafe(F.col("d.id")), "left")
    .select("s.*", F.col("d.segment"), F.col("d.country"), F.col("d.name"))
)

metrics = (
    enriched_subscriptions
    .select(
        "*",
        F.ntile(3).over(w3).alias("age_bucket"),
        F.first("last7_sum_days_since_end_date", ignorenulls=True).over(w4).alias("first_known_last7_sum_days_since_end_date"),
        F.row_number().over(w5).alias("row_seq"),
    )
)

enriched_subscriptions_2 = (
    metrics
    .alias("src")
    .join(df_customers.alias("b"), F.col("src.customer_id") == F.col("b.id"), "inner")
    .select(
        "src.*",
        F.col("b.age").alias("customer_age"),
        F.col("b.city"),
        F.col("b.signup_date"),
    )
)

result = (
    enriched_subscriptions_2
    .alias("e")
    .join(df_customers.alias("c"), F.col("e.customer_id") == F.col("c.id"), "inner")
    .rollup(
        F.col("c.segment").alias("customer_segment"),
        F.col("e.city").alias("subscription_city"),
    )
    .agg(
        F.count("*").alias("rows_total"),
        F.sum(F.col("e.days_since_end_date")).alias("total_days_since_end_date"),
        F.round(F.sum(F.col("e.row_seq")), 2).alias("total_row_seq"),
        F.grouping_id().alias("gid"),
    )
    .orderBy(F.col("total_row_seq").desc_nulls_first())
)
