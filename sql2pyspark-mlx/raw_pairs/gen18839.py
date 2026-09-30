from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy(F.col("signup_date").desc_nulls_first(), "id")
w2 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.started_at").desc(), F.col("c.id").desc())
w3 = Window.partitionBy("city", "country").orderBy("credit_limit_per_age", "id").rowsBetween(-5, Window.currentRow)
w4 = Window.partitionBy("session_country").orderBy("is_de", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_customers
    .filter((F.col("signup_date") >= F.lit("2023-03-16").cast("date")) & F.col("city").isNotNull())
    .select(
        "id",
        "name",
        "email",
        "country",
        "city",
        "signup_date",
        "is_active",
        "credit_limit",
        "age",
        F.when(F.col("country") == "DE", 1).otherwise(0).alias("is_de"),
        F.round((F.col("credit_limit") / F.col("age")), 2).alias("credit_limit_per_age"),
    )
)

latest = (
    filtered
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
)

with_last = (
    latest
    .alias("s")
    .join(df_sessions.alias("c"), (F.col("c.customer_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.country").alias("session_country"))
)

with_windows = (
    with_last
    .select(
        "id",
        "name",
        "email",
        "country",
        "signup_date",
        "is_active",
        "credit_limit",
        "age",
        "credit_limit_per_age",
        "dedup_rank",
        "session_country",
        F.avg("credit_limit").over(w3).alias("credit_limit_avg_5"),
        F.sum("age").over(w4).alias("running_age"),
    )
)

result = (
    with_windows
    .rollup(F.date_trunc("YEAR", "signup_date").alias("year_start"), "country")
    .agg(
        F.sum(F.when(F.col("session_country") == "US", F.col("age")).otherwise(0)).alias("us_age"),
        F.count_if(F.col("country") == "US").alias("us_events"),
        F.corr("credit_limit", "age").alias("corr_credit_limit_age"),
    )
    .filter(F.col("us_age") < 1000000)
    .orderBy(F.col("corr_credit_limit_age").desc_nulls_first())
)
