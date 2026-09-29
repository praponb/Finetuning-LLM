from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy("age", "id")
w2 = Window.partitionBy("segment").orderBy("credit_limit", "id")
w3 = Window.partitionBy("segment").orderBy(F.col("credit_limit_times_age").desc_nulls_first())
w4 = Window.partitionBy("country").orderBy("credit_limit", "id")
w_ordered = Window.partitionBy("country").orderBy("credit_limit_times_age", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_grp = Window.partitionBy("segment", "country").orderBy("cumulative_age", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy("segment").orderBy(F.col("credit_limit").asc_nulls_first())
w8 = Window.partitionBy("country")
w9 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.started_at").desc(), F.col("c.id").desc())
w10 = Window.partitionBy("segment").orderBy("credit_limit_bucket_filled", "id")
w11 = Window.partitionBy("segment")

latest = (
    df_customers
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
)

cleaned = (
    latest
    .filter(F.col("name").like("%Ross%"))
    .select(
        "id",
        "name",
        "email",
        "country",
        "segment",
        "signup_date",
        "is_active",
        "credit_limit",
        "age",
        (F.col("credit_limit") * F.col("age")).alias("credit_limit_times_age"),
        F.coalesce("city", F.lit("DIRECT")).alias("city_clean"),
    )
)

with_windows = (
    cleaned
    .select(
        "*",
        F.ntile(10).over(w2).alias("credit_limit_bucket"),
        F.cume_dist().over(w3).alias("credit_limit_times_age_cume_dist"),
        F.lead("credit_limit_times_age", 2).over(w4).alias("next_credit_limit_times_age"),
        F.sum("age").over(w_ordered).alias("cumulative_age"),
    )
)

cleaned_customers = (
    with_windows
    .filter(F.col("age") == 38)
    .select(
        "id",
        "email",
        "country",
        "segment",
        "signup_date",
        "credit_limit",
        "age",
        "credit_limit_times_age",
        "city_clean",
        "credit_limit_bucket",
        "credit_limit_times_age_cume_dist",
        "next_credit_limit_times_age",
        "cumulative_age",
        F.coalesce("credit_limit_bucket", F.lit(0)).alias("credit_limit_bucket_filled"),
        F.length("country").alias("country_length"),
    )
)

with_windows_customers = (
    cleaned_customers
    .withColumn("last_known_credit_limit_bucket_filled", F.last("credit_limit_bucket_filled", ignorenulls=True).over(w_grp))
    .withColumn("credit_limit_rank", F.rank().over(w7))
    .withColumn("cumulative_age_share", F.round((F.col("cumulative_age") / F.sum("cumulative_age").over(w8)), 4))
)

latest_match = (
    with_windows_customers
    .alias("s")
    .join(df_sessions.alias("c"), (F.col("c.customer_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w9))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.started_at"))
)

result = (
    latest_match
    .withColumn("credit_limit_bucket_filled_bucket", F.ntile(3).over(w10))
    .withColumn("segment_max_last_known_credit_limit_bucket_filled", F.max("last_known_credit_limit_bucket_filled").over(w11))
)
