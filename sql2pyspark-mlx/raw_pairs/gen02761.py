from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("monthly_fee", "id").rowsBetween(-6, Window.currentRow)
w2 = Window.partitionBy("status").orderBy(F.col("last7_count_monthly_fee").desc_nulls_last(), "id")

filtered = (
    df_subscriptions
    .filter((F.col("start_date") > F.to_date(F.lit("2023-06-26"))) & F.col("customer_id").isNotNull())
    .withColumn("plan_title", F.initcap(F.trim("plan")))
    .withColumn("weeks_end_date_to_start_date", (F.datediff("start_date", "end_date") / 7))
)

enriched = (
    filtered
    .alias("e")
    .join(df_customers.alias("b"), F.col("e.customer_id") == F.col("b.id"), "left")
    .select(
        "e.id",
        "e.customer_id",
        "e.plan",
        "e.end_date",
        "e.monthly_fee",
        "e.status",
        "e.plan_title",
        "e.weeks_end_date_to_start_date",
        F.col("b.age"),
    )
)

scored = (
    enriched
    .filter(F.col("customer_id") == 32)
    .select(
        "id",
        "status",
        "plan_title",
        "weeks_end_date_to_start_date",
        "age",
        F.max("monthly_fee").over(w).alias("last7_max_monthly_fee"),
        F.avg("monthly_fee").over(w).alias("last7_avg_monthly_fee"),
        F.count("monthly_fee").over(w).alias("last7_count_monthly_fee"),
        F.min("monthly_fee").over(w).alias("last7_min_monthly_fee"),
    )
)

filtered_subscriptions = (
    scored
    .filter(F.col("weeks_end_date_to_start_date") <= 30)
    .select(
        "id",
        "status",
        "plan_title",
        "age",
        "last7_max_monthly_fee",
        "last7_avg_monthly_fee",
        "last7_count_monthly_fee",
        F.when(F.col("last7_avg_monthly_fee") >= 222, "High").when(F.col("last7_avg_monthly_fee") >= 105, "Medium").otherwise("Low").alias("last7_avg_monthly_fee_band"),
    )
)

ranked = (
    filtered_subscriptions
    .withColumn("dedup_rank", F.row_number().over(w2))
    .filter(F.col("dedup_rank") == 1)
)

stats = (
    ranked
    .withColumn("age_tier", F.when(F.col("age") >= 61, "HIGH").otherwise("LOW"))
    .groupingSets([["status", "age_tier"], ["status"]], "status", "age_tier")
    .agg(
        F.percentile_approx("last7_max_monthly_fee", [0.25, 0.5, 0.75]).alias("last7_max_monthly_fee_quartiles"),
        F.percentile_approx("last7_max_monthly_fee", [0.25, 0.5, 0.75]).alias("last7_max_monthly_fee_quartiles_2"),
        F.count_if(F.col("status") == "ACTIVE").alias("active_events"),
        F.count(F.when(F.col("status") == "EXPIRED", 1)).alias("expired_rows"),
        F.grouping("status").alias("gid"),
    )
    .filter(F.col("expired_rows") >= 1)
)

result = (
    stats
    .select(
        "status",
        "age_tier",
        "last7_max_monthly_fee_quartiles",
        "active_events",
        "gid",
        F.when(F.col("status").rlike("^(CANCELLED|EXPIRED)$"), "EXTENDED").when(F.col("status").rlike("^(ACTIVE|PAUSED)$"), "OTHER_ZONE").otherwise("GROUP_B").alias("status_zone"),
    )
    .orderBy(F.col("gid").desc())
)
