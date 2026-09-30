from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.partitionBy("browser").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy(F.col("started_at").desc(), "id")
w_running = Window.partitionBy("customer_country").orderBy("age", "id").rowsBetween(-6, Window.currentRow)

cleaned = (
    df_sessions
    .filter((F.col("started_at") > F.lit("2023-04-10 16:00:00").cast("timestamp")) & F.col("browser").isNotNull())
    .withColumn("browser_country", F.concat("browser", F.lit("/"), "country"))
    .withColumn("browser_clean", F.coalesce("browser", F.lit("UNKNOWN")))
)

metrics = (
    cleaned
    .filter(F.col("customer_id").isNull())
    .select(
        "*",
        F.first("country", ignorenulls=True).over(w_ordered).alias("first_known_country"),
        F.row_number().over(w2).alias("event_seq"),
    )
)

joined = (
    metrics
    .alias("src")
    .join(df_customers.alias("c"), F.col("src.customer_id").eqNullSafe(F.col("c.id")), "left")
    .select(
        "src.id",
        "src.customer_id",
        "src.device",
        "src.browser",
        "src.country",
        "src.browser_country",
        "src.first_known_country",
        F.col("c.city"),
        F.col("c.age"),
        F.col("c.country").alias("customer_country"),
    )
)

filtered = (
    joined
    .filter(F.col("browser").isNotNull() & (~F.col("customer_country").eqNullSafe(F.lit("TH"))))
    .select(
        "*",
        F.upper("city").alias("city_upper"),
        F.coalesce("country", F.lit("DIRECT")).alias("country_filled"),
    )
)

scored = (
    filtered
    .select(
        "*",
        F.min("age").over(w_running).alias("last7_min_age"),
        F.count("age").over(w_running).alias("last7_count_age"),
        F.sum("age").over(w_running).alias("last7_sum_age"),
        F.avg("age").over(w_running).alias("last7_avg_age"),
    )
)

result = (
    scored
    .select(
        "id",
        "customer_id",
        "browser",
        "country",
        "browser_country",
        "first_known_country",
        "city",
        "age",
        "country_filled",
    )
    .orderBy(F.col("country").asc_nulls_first(), "browser")
)
