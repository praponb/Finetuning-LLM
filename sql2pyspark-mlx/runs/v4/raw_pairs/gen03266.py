from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("start_date", "id")
w2 = Window.partitionBy("customer_id").orderBy("start_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("segment").orderBy("row_seq", "id")
w_grp = Window.partitionBy("segment").orderBy("end_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("segment").orderBy("monthly_fee_rounded", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("segment")
w7 = Window.partitionBy("segment").orderBy("last_known_days_start_date_to_end_date_minus_days_start_date_to_end_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.partitionBy("segment").orderBy(F.col("row_seq_p95").desc())

cleaned = (
    df_subscriptions
    .filter((F.col("start_date") >= F.lit("2024-02-24").cast("date")) & F.col("start_date").between("2023-08-01", "2025-01-26"))
    .select(
        "id",
        "customer_id",
        "start_date",
        "end_date",
        "monthly_fee",
        F.datediff("end_date", "start_date").alias("days_start_date_to_end_date"),
    )
)

metrics = (
    cleaned
    .select(
        "*",
        F.row_number().over(w).alias("row_seq"),
        F.last("days_start_date_to_end_date", ignorenulls=True).over(w2).alias("last_known_days_start_date_to_end_date"),
    )
)

enriched = (
    metrics
    .alias("src")
    .join(df_customers.alias("c"), F.col("src.customer_id") == F.col("c.id"), "inner")
    .select("src.*", F.col("c.segment"))
)

filtered = (
    enriched
    .filter(F.col("customer_id").isNotNull())
    .select(
        "id",
        "end_date",
        "monthly_fee",
        "days_start_date_to_end_date",
        "row_seq",
        "last_known_days_start_date_to_end_date",
        "segment",
        (F.col("last_known_days_start_date_to_end_date") - F.col("days_start_date_to_end_date")).alias("last_known_days_start_date_to_end_date_minus_days_start_date_to_end_date"),
        F.coalesce("segment", F.lit("N/A")).alias("segment_clean"),
    )
)

base = (
    filtered
    .filter((F.col("end_date") > F.lit("2023-06-06").cast("date")) & F.col("segment").isNotNull())
    .withColumn("monthly_fee_rounded", F.round("monthly_fee", 1))
)

with_windows = (
    base
    .withColumn("next_last_known_days_start_date_to_end_date_minus_days_start_date_to_end_date", F.lead("last_known_days_start_date_to_end_date_minus_days_start_date_to_end_date", 2, 0).over(w3))
    .withColumn("first_known_last_known_days_start_date_to_end_date_minus_days_start_date_to_end_date", F.first("last_known_days_start_date_to_end_date_minus_days_start_date_to_end_date", ignorenulls=True).over(w_grp))
    .withColumn("first_known_monthly_fee_rounded", F.first("monthly_fee_rounded", ignorenulls=True).over(w5))
)

enriched_subscriptions = (
    with_windows
    .withColumn("segment_sum_first_known_monthly_fee_rounded", F.sum("first_known_monthly_fee_rounded").over(w6))
    .withColumn("last_known_first_known_last_known_days_start_date_to_end_date_minus_days_start_date_to_end_date", F.last("first_known_last_known_days_start_date_to_end_date_minus_days_start_date_to_end_date", ignorenulls=True).over(w7))
)

summary = (
    enriched_subscriptions
    .groupBy("segment", F.date_trunc("WEEK", "end_date").alias("week_start"))
    .agg(
        F.avg(F.when(F.col("segment") == "Consumer", F.col("first_known_last_known_days_start_date_to_end_date_minus_days_start_date_to_end_date"))).alias("consumer_avg_first_known_last_known_days_start_date_to_end_date_minus_days_start_date_to_end_date"),
        F.percentile_approx("row_seq", 0.95).alias("row_seq_p95"),
        F.count(F.when(F.col("segment") == "Corporate", 1)).alias("corporate_rows"),
        F.count_if(F.col("segment") == "SMB").alias("smb_events"),
    )
    .withColumn("row_seq_p95_rank", F.rank().over(w8))
)

result = (
    summary
    .select(
        "segment",
        "week_start",
        "row_seq_p95",
        "corporate_rows",
        "row_seq_p95_rank",
        F.when(F.col("segment").rlike("^(SMB|Consumer)$"), "GROUP_B").when(F.col("segment").rlike("^(Corporate)$"), "EXTENDED").otherwise("CORE").alias("segment_zone"),
    )
)
