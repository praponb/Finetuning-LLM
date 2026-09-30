from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy(F.col("signup_date").desc(), "id")

base = (
    df_sessions
    .filter((F.col("started_at") >= F.lit("2023-07-31 09:00:00").cast("timestamp")) & (F.col("browser") == "Chrome"))
    .select(
        "id",
        "customer_id",
        "browser",
        "country",
        F.concat_ws("/", "device", "browser").alias("device_browser_key"),
        F.substring("device", 1, 4).alias("device_prefix"),
    )
)

joined = (
    base
    .alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "left")
    .filter(F.col("c.city") != "Singapore")
    .select("s.*", F.col("c.email"), F.col("c.signup_date"))
)

not_flagged = (
    joined
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")) & (F.col("c.event_time") <= F.col("s.signup_date")), "left_semi")
)

ranked = (
    not_flagged
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
    .select("id", "device_browser_key", "signup_date")
)

result = (
    ranked
    .withColumn("activity_month", F.date_trunc("MONTH", "signup_date"))
    .withColumn("year_start", F.date_trunc("YEAR", "signup_date"))
    .groupingSets(
        [["activity_month", "year_start"], ["activity_month"]],
        "activity_month",
        "year_start",
    )
    .agg(
        F.count("*").alias("num_rows"),
        F.count("*").alias("num_rows_2"),
        F.countDistinct("id").alias("distinct_ids"),
        F.grouping("activity_month").alias("gid"),
    )
    .orderBy(F.col("num_rows").desc())
)
