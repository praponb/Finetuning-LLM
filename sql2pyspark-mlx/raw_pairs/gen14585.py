from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy(F.col("credit_limit").asc_nulls_last())
w2 = Window.partitionBy("segment").orderBy("credit_limit", "id")
w3 = Window.partitionBy(F.month("signup_date"), "segment").orderBy("next_credit_limit", "id")
w4 = Window.partitionBy(F.month("signup_date"), "city").orderBy("next_credit_limit", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy(F.date_trunc("WEEK", "signup_date")).orderBy(F.col("event_seq_adjusted").desc_nulls_last())

latest = (
    df_customers
    .withColumn("latest_rank", F.rank().over(w))
    .filter(F.col("latest_rank") == 1)
)

with_windows = (
    latest
    .select(
        "*",
        F.lead("credit_limit").over(w2).alias("next_credit_limit"),
        F.lead("age").over(w2).alias("next_age"),
        F.lag("age").over(w2).alias("prev_age"),
        F.row_number().over(w2).alias("event_seq"),
    )
)

valid = (
    with_windows
    .filter(F.col("signup_date") >= F.lit("2023-04-03").cast("date"))
    .select(
        "id",
        "name",
        "email",
        "country",
        "city",
        "segment",
        "signup_date",
        "credit_limit",
        "latest_rank",
        "next_credit_limit",
        "prev_age",
        "event_seq",
        F.round((F.col("event_seq") * 1.15), 2).alias("event_seq_adjusted"),
    )
)

enriched = (
    valid
    .select(
        "id",
        "name",
        "email",
        "credit_limit",
        "prev_age",
        F.row_number().over(w3).alias("event_seq_2"),
        F.sum("prev_age").over(w4).alias("cumulative_prev_age"),
        F.cume_dist().over(w5).alias("event_seq_adjusted_cume_dist"),
    )
)

result = (
    enriched
    .rollup(
        F.when(F.col("prev_age") >= 63, "PREMIUM").otherwise("STANDARD").alias("prev_age_tier"),
    )
    .agg(
        F.percentile_approx("event_seq_2", 0.95).alias("event_seq_2_p95"),
        F.round(F.avg("prev_age"), 2).alias("avg_prev_age"),
    )
    .orderBy(F.col("event_seq_2_p95").desc())
)
