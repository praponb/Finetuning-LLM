from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country", "city").orderBy(F.col("credit_limit").asc_nulls_first(), "id")
w2 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.started_at").desc(), F.col("c.id").desc())
w_running = Window.partitionBy("country", "segment").orderBy("started_at", "id").rowsBetween(-5, -1)
w4 = Window.partitionBy("country", "segment").orderBy("signup_date_minus_60d", "id")
w5 = Window.partitionBy("country").orderBy("age_avg_5_prior", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("city").orderBy("age", "id")
w7 = Window.partitionBy("country").orderBy(F.col("age_avg_5_prior").desc(), "id")

filtered = (
    df_customers
    .filter(F.col("signup_date") > F.lit("2023-08-24").cast("date"))
    .withColumn("signup_date_minus_60d", F.date_sub("signup_date", 60))
    .withColumn("clean_country", F.trim("country"))
)

deduped = (
    filtered
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
)

filtered_customers = (
    deduped
    .filter(F.col("signup_date_minus_60d") < "2024-06-01")
    .withColumn("city_filled", F.coalesce("city", F.lit("Unknown")))
)

kept = (
    filtered_customers
    .alias("s")
    .join(df_orders.alias("x"), (F.col("x.customer_id") == F.col("s.id")) & (F.col("x.order_date") >= F.col("s.signup_date_minus_60d")), "left_semi")
)

latest_match = (
    kept
    .alias("s")
    .join(df_sessions.alias("c"), (F.col("c.customer_id") == F.col("s.id")) & (F.col("c.started_at") <= F.col("s.signup_date_minus_60d")), "inner")
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.started_at"))
)

with_windows = (
    latest_match
    .filter(F.col("age").between(41, 62))
    .select(
        "id",
        "name",
        "country",
        "city",
        "credit_limit",
        "age",
        "signup_date_minus_60d",
        "city_filled",
        "started_at",
        F.avg("age").over(w_running).alias("age_avg_5_prior"),
        F.lead("age", 1).over(w4).alias("next_age"),
    )
)

enriched = (
    with_windows
    .withColumn("first_known_credit_limit", F.first("credit_limit", ignorenulls=True).over(w5))
    .withColumn("prev_next_age", F.lag("next_age", 1).over(w6))
    .withColumn("seq_no", F.row_number().over(w7))
)

result = (
    enriched
    .groupBy("city")
    .pivot("country", ["JP", "UK"])
    .agg(F.max("age_avg_5_prior").alias("amount"))
)
