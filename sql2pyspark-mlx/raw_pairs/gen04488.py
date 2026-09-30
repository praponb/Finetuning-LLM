from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy(F.col("signup_date").desc(), "id")
w2 = Window.partitionBy(F.year("signup_date"), "country").orderBy("signup_date", "id")
w3 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.shipping_fee").desc(), F.col("c.id").desc())

scored = (
    df_customers
    .withColumn("row_seq", F.row_number().over(w))
    .withColumn("seq_no", F.row_number().over(w2))
)

cleaned = (
    scored
    .filter(F.col("signup_date") >= F.to_date(F.lit("2023-02-24")))
    .select(
        "id",
        "name",
        "email",
        "country",
        "city",
        "segment",
        "signup_date",
        "is_active",
        "credit_limit",
        "age",
        "seq_no",
        (F.col("seq_no") + F.col("row_seq")).alias("seq_no_plus_row_seq"),
        F.coalesce("city", F.lit("NONE")).alias("city_clean"),
    )
)

eligible = (
    cleaned
    .alias("s")
    .join(df_sessions.alias("x"), (F.col("x.customer_id") == F.col("s.id")) & (F.col("x.started_at") < F.col("s.signup_date")), "left_anti")
)

latest_match = (
    eligible
    .alias("s")
    .join(df_orders.alias("c"), (F.col("c.customer_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.status").alias("order_status"))
)

not_flagged = (
    latest_match
    .alias("s")
    .join(df_sessions.alias("c"), (F.col("c.customer_id") == F.col("s.id")) & (F.col("c.started_at") >= "2023-04-01"), "left_semi")
)

not_flagged_customers = (
    not_flagged
    .alias("s")
    .join(df_accounts.alias("ch"), (F.col("ch.customer_id") == F.col("s.id")) & (F.col("ch.opened_date") <= F.col("s.signup_date")), "left_anti")
)

result = (
    not_flagged_customers
    .withColumn("week_start", F.date_trunc("WEEK", "signup_date"))
    .cube("week_start")
    .agg(
        F.round(F.avg("seq_no_plus_row_seq"), 2).alias("avg_seq_no_plus_row_seq"),
        F.count("*").alias("event_count"),
        F.sum("credit_limit").alias("total_credit_limit"),
        F.count(F.when(F.col("city") == "New York", 1)).alias("new_york_count"),
        F.percentile_approx("age", [0.25, 0.5, 0.75]).alias("age_quartiles"),
        F.grouping_id().alias("grouping_level"),
    )
    .filter(F.col("avg_seq_no_plus_row_seq") > 0)
)
