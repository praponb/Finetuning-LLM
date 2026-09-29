from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("weeks_since_opened_date", "id")
w_running = Window.partitionBy("status").orderBy("balance_times_weeks_since_opened_date", "id").rowsBetween(-6, -1)
w3 = Window.partitionBy("customer_id").orderBy("balance_times_weeks_since_opened_date", "id").rowsBetween(-2, -1)
w4 = Window.partitionBy("customer_id").orderBy("balance", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("customer_id").orderBy(F.col("balance_times_weeks_since_opened_date").desc_nulls_first())
w6 = Window.partitionBy("status").orderBy("balance_times_weeks_since_opened_date_percent_rank", "id").rowsBetween(-6, -1)
w7 = Window.partitionBy("status", "customer_id").orderBy("balance_times_weeks_since_opened_date_percent_rank", "id")
w_recent = Window.partitionBy("customer_id").orderBy("balance_times_weeks_since_opened_date_percent_rank_bucket", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w9 = Window.partitionBy("customer_id").orderBy(F.col("weeks_since_opened_date_avg_2_prior").desc(), "id")
w10 = Window.partitionBy("customer_id").orderBy("weeks_since_opened_date_avg_2_prior", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w11 = Window.partitionBy("customer_id").orderBy("balance_times_weeks_since_opened_date", "id")

cleaned = (
    df_accounts
    .filter((F.col("opened_date") > F.to_date(F.lit("2023-09-06"))) & F.col("status").isNotNull())
    .select(
        "id",
        "customer_id",
        "balance",
        "opened_date",
        "status",
        (F.datediff(F.current_date(), "opened_date") / 7).alias("weeks_since_opened_date"),
        F.coalesce("status", F.lit("DIRECT")).alias("status_clean"),
    )
)

valid = (
    cleaned
    .filter(F.col("status") == "CLOSED")
    .select(
        "id",
        "customer_id",
        "balance",
        "status",
        "weeks_since_opened_date",
        "status_clean",
        (F.col("balance") * F.col("weeks_since_opened_date")).alias("balance_times_weeks_since_opened_date"),
    )
)

metrics = (
    valid
    .filter(F.col("status") == "OPEN")
    .select(
        "id",
        "customer_id",
        "balance",
        "status",
        "weeks_since_opened_date",
        "balance_times_weeks_since_opened_date",
        F.lead("balance", 1).over(w).alias("next_balance"),
        F.avg("balance_times_weeks_since_opened_date").over(w_running).alias("balance_times_weeks_since_opened_date_avg_6_prior"),
        F.avg("weeks_since_opened_date").over(w3).alias("weeks_since_opened_date_avg_2_prior"),
        F.sum("balance_times_weeks_since_opened_date").over(w4).alias("cumulative_balance_times_weeks_since_opened_date"),
        F.percent_rank().over(w5).alias("balance_times_weeks_since_opened_date_percent_rank"),
    )
)

scored = (
    metrics
    .withColumn("balance_times_weeks_since_opened_date_percent_rank_avg_6_prior", F.avg("balance_times_weeks_since_opened_date_percent_rank").over(w6))
    .withColumn("balance_times_weeks_since_opened_date_percent_rank_bucket", F.ntile(3).over(w7))
)

joined = (
    scored
    .alias("src")
    .join(df_customers.alias("p"), F.col("src.customer_id") == F.col("p.id"), "left")
    .select(
        "src.id",
        "src.customer_id",
        "src.balance_times_weeks_since_opened_date",
        "src.weeks_since_opened_date_avg_2_prior",
        "src.balance_times_weeks_since_opened_date_percent_rank_bucket",
        F.col("p.age"),
    )
)

enriched = (
    joined
    .withColumn("cumulative_balance_times_weeks_since_opened_date_percent_rank_bucket", F.sum("balance_times_weeks_since_opened_date_percent_rank_bucket").over(w_recent))
    .withColumn("seq_no", F.row_number().over(w9))
    .withColumn("last_known_age", F.last("age", ignorenulls=True).over(w10))
    .withColumn("event_seq", F.row_number().over(w11))
)

cleaned_accounts = (
    enriched
    .filter(F.col("customer_id").isNotNull() & (F.col("age") != 65))
    .withColumn("event_seq_tier", F.when(F.col("event_seq") >= 9, "High").when(F.col("event_seq") >= 4, "Medium").otherwise("Low"))
)

result = (
    cleaned_accounts
    .withColumn("cumulative_balance_times_weeks_since_opened_date_percent_rank_bucket_tier", F.when(F.col("cumulative_balance_times_weeks_since_opened_date_percent_rank_bucket") >= 45, "LARGE").otherwise("SMALL"))
    .groupingSets(
        [["seq_no", "cumulative_balance_times_weeks_since_opened_date_percent_rank_bucket_tier"], ["seq_no"]],
        "seq_no",
        "cumulative_balance_times_weeks_since_opened_date_percent_rank_bucket_tier",
    )
    .agg(
        F.percentile_approx("last_known_age", [0.25, 0.5, 0.75]).alias("last_known_age_quartiles"),
        F.round(F.avg("last_known_age"), 2).alias("avg_last_known_age"),
        F.sum("balance_times_weeks_since_opened_date").alias("total_balance_times_weeks_since_opened_date"),
        F.percentile_approx("balance_times_weeks_since_opened_date", [0.25, 0.5, 0.75]).alias("balance_times_weeks_since_opened_date_quartiles"),
        F.grouping("seq_no").alias("grouping_level"),
    )
    .orderBy(F.col("avg_last_known_age").desc())
)
