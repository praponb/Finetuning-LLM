from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan").orderBy(F.col("days_since_end_date").desc_nulls_first(), "id")
w2 = Window.partitionBy("plan").orderBy(F.col("days_since_end_date").desc())
w_hist = Window.orderBy("days_since_end_date", "id").rowsBetween(-3, Window.currentRow)
w4 = Window.orderBy("days_since_end_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.orderBy("rank_in_group", "id")

valid = (
    df_subscriptions
    .filter((F.col("start_date") >= F.to_date(F.lit("2023-05-30"))) & F.col("customer_id").isNotNull())
    .select(
        "id",
        "customer_id",
        "plan",
        "end_date",
        "monthly_fee",
        F.datediff(F.current_date(), "end_date").alias("days_since_end_date"),
    )
)

ranked = (
    valid
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
)

ranked_subscriptions = (
    ranked
    .withColumn("rn", F.rank().over(w2))
    .filter(F.col("rn") == 1)
    .select("id", "monthly_fee", "days_since_end_date", "rank_in_group")
)

metrics = (
    ranked_subscriptions
    .select(
        "id",
        "rank_in_group",
        F.avg("days_since_end_date").over(w_hist).alias("days_since_end_date_avg_3"),
        F.first("days_since_end_date", ignorenulls=True).over(w4).alias("first_known_days_since_end_date"),
        F.lead("monthly_fee", 1).over(w5).alias("next_monthly_fee"),
    )
)

result = (
    metrics
    .cube(
        F.when(F.col("days_since_end_date_avg_3") >= 88, "HIGH").otherwise("LOW").alias("days_since_end_date_avg_3_tier"),
    )
    .agg(
        F.percentile_approx("days_since_end_date_avg_3", [0.25, 0.5, 0.75]).alias("days_since_end_date_avg_3_quartiles"),
        F.sum("next_monthly_fee").alias("total_next_monthly_fee"),
        F.count("*").alias("num_rows"),
    )
    .orderBy(F.col("num_rows").desc())
)
