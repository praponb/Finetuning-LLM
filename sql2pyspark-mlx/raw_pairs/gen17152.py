from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("monthly_fee").desc())
w_recent = Window.partitionBy("status", "plan").orderBy("row_num", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy(F.year("start_date"))
w4 = Window.partitionBy("country").orderBy("running_sum_months_since_end_date", "id")
w5 = Window.partitionBy("customer_id", "country").orderBy(F.col("days_start_date_to_end_date").desc_nulls_last())

base = (
    df_subscriptions
    .filter(F.col("start_date") >= F.to_date(F.lit("2023-07-14")))
    .select(
        "id",
        "customer_id",
        "plan",
        "start_date",
        "monthly_fee",
        "status",
        F.datediff("end_date", "start_date").alias("days_start_date_to_end_date"),
        (F.datediff(F.current_date(), "end_date") / 30).alias("months_since_end_date"),
    )
)

latest = (
    base
    .withColumn("row_num", F.rank().over(w))
    .filter(F.col("row_num") == 1)
)

metrics = (
    latest
    .select(
        "*",
        F.sum("months_since_end_date").over(w_recent).alias("running_sum_months_since_end_date"),
        F.max("months_since_end_date").over(w_recent).alias("running_max_months_since_end_date"),
        F.avg("months_since_end_date").over(w_recent).alias("running_avg_months_since_end_date"),
        F.min("months_since_end_date").over(w_recent).alias("running_min_months_since_end_date"),
        F.avg("months_since_end_date").over(w3).alias("start_date_year_avg_months_since_end_date"),
    )
)

enriched = (
    metrics
    .alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "inner")
    .filter(F.col("c.is_active"))
    .select(
        "s.id",
        "s.customer_id",
        "s.days_start_date_to_end_date",
        "s.months_since_end_date",
        "s.row_num",
        "s.running_sum_months_since_end_date",
        "s.running_max_months_since_end_date",
        "s.running_avg_months_since_end_date",
        "s.start_date_year_avg_months_since_end_date",
        F.col("c.age"),
        F.col("c.city"),
        F.col("c.country"),
    )
)

cleaned = (
    enriched
    .filter(F.col("customer_id").isNotNull() & (~F.col("country").eqNullSafe(F.lit("JP"))))
    .withColumn("age_whole", F.floor("age"))
)

enriched_subscriptions = (
    cleaned
    .alias("e")
    .join(df_customers.alias("c"), F.col("e.customer_id") == F.col("c.id"), "inner")
    .select("e.*", F.col("c.email"))
)

enriched_subscriptions_2 = (
    enriched_subscriptions
    .withColumn("next_running_max_months_since_end_date", F.lead("running_max_months_since_end_date").over(w4))
    .withColumn("prev_days_start_date_to_end_date", F.lag("days_start_date_to_end_date").over(w4))
    .withColumn("seq_no", F.row_number().over(w4))
    .withColumn("days_start_date_to_end_date_cume_dist", F.cume_dist().over(w5))
)

totals = (
    enriched_subscriptions_2
    .cube(
        F.when(F.col("seq_no") >= 9, "HIGH").otherwise("LOW").alias("seq_no_tier"),
        "customer_id",
    )
    .agg(
        F.percentile_approx("months_since_end_date", [0.25, 0.5, 0.75]).alias("months_since_end_date_quartiles"),
        F.round(F.sum("running_avg_months_since_end_date") / F.nullif(F.count(F.when(F.col("city") == "Berlin", 1)), F.lit(0)), 2).alias("running_avg_months_since_end_date_per_berlin"),
        F.percentile_approx("age_whole", [0.25, 0.5, 0.75]).alias("age_whole_quartiles"),
        F.size(F.collect_set("country")).alias("num_countries"),
        F.avg(F.when(F.col("country") == "US", F.col("age"))).alias("us_avg_age"),
        F.round(F.sum("age_whole") / F.nullif(F.count(F.when(F.col("city") == "London", 1)), F.lit(0)), 2).alias("age_whole_per_london"),
    )
)

result = (
    totals
    .select(
        "seq_no_tier",
        "running_avg_months_since_end_date_per_berlin",
        "age_whole_quartiles",
        "us_avg_age",
        "age_whole_per_london",
    )
    .orderBy(F.col("running_avg_months_since_end_date_per_berlin").asc_nulls_last())
)
