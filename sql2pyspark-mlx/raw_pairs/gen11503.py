from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan").orderBy("end_date", "id")
w2 = Window.partitionBy(F.date_trunc("MONTH", "end_date"), "status").orderBy("monthly_fee", "id")
w3 = Window.partitionBy("status").orderBy(F.col("end_date").desc_nulls_first(), "id")

cleaned = (
    df_subscriptions
    .filter(F.col("start_date") > F.lit("2023-04-28").cast("date"))
    .select("*", F.lower("plan").alias("plan_lower"))
)

enriched = (
    cleaned
    .select(
        "id",
        "end_date",
        "monthly_fee",
        "status",
        F.lag("monthly_fee").over(w).alias("prev_monthly_fee"),
        F.row_number().over(w).alias("row_seq"),
        F.ntile(5).over(w2).alias("monthly_fee_bucket"),
    )
)

ranked = (
    enriched
    .withColumn("rank_in_group", F.row_number().over(w3))
    .filter(F.col("rank_in_group") == 1)
)

base = (
    ranked
    .filter((F.col("end_date") >= F.lit("2023-06-01").cast("date")) & (F.col("monthly_fee") >= 210))
    .select("*", F.datediff(F.current_date(), "end_date").alias("days_since_end_date"))
)

cleaned_subscriptions = (
    base
    .filter(F.col("end_date") >= F.to_date(F.lit("2023-09-15")))
    .select(
        "id",
        "end_date",
        "monthly_fee",
        "status",
        "prev_monthly_fee",
        "row_seq",
        "monthly_fee_bucket",
        "rank_in_group",
        "days_since_end_date",
        F.least("row_seq", "monthly_fee").alias("least_row_seq_monthly_fee"),
    )
)

result = (
    cleaned_subscriptions
    .select(
        "id",
        "end_date",
        "status",
        "row_seq",
        "monthly_fee_bucket",
        "rank_in_group",
        "days_since_end_date",
        F.when(F.col("status").rlike("^(EXPIRED|ACTIVE)$"), "GROUP_A").when(F.col("status").rlike("^(CANCELLED|PAUSED)$"), "GROUP_B").otherwise("CORE").alias("status_zone"),
    )
    .orderBy("rank_in_group")
)
