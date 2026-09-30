from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city", "segment").orderBy(F.col("credit_limit").asc_nulls_first(), "id")
w2 = Window.partitionBy("segment", "country").orderBy("credit_limit", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("city")

cleaned = (
    df_customers
    .filter(F.col("signup_date") > F.lit("2023-08-14").cast("date"))
    .select(
        "id",
        "name",
        "country",
        "city",
        "segment",
        "signup_date",
        "is_active",
        "credit_limit",
        "age",
        F.concat_ws("/", "segment", "city").alias("segment_city_key"),
        F.coalesce("city", F.lit("OTHER")).alias("city_filled"),
    )
)

deduped = (
    cleaned
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
    .drop("dedup_rank")
)

scored = (
    deduped
    .select(
        "id",
        "country",
        "city",
        "credit_limit",
        "age",
        F.min("credit_limit").over(w2).alias("running_min_credit_limit"),
        F.avg("credit_limit").over(w2).alias("running_avg_credit_limit"),
        F.sum("credit_limit").over(w2).alias("running_sum_credit_limit"),
        F.round((F.col("age") / F.sum("age").over(w3)), 4).alias("age_share"),
    )
)

filtered = (
    scored
    .filter(F.col("city").isNotNull() & (F.col("running_sum_credit_limit") > 74000))
    .select("*", F.concat_ws("-", "country", "city").alias("country_city_key"))
)

result = (
    filtered
    .withColumn("age_tier", F.when(F.col("age") >= 70, "PREMIUM").otherwise("STANDARD"))
    .cube("city", "age_tier")
    .agg(
        F.round(F.sum("running_sum_credit_limit") / F.nullif(F.count(F.when(F.col("country") == "US", 1)), F.lit(0)), 2).alias("running_sum_credit_limit_per_us"),
        F.avg(F.when(F.col("country") == "TH", F.col("running_sum_credit_limit"))).alias("th_avg_running_sum_credit_limit"),
        F.round(F.sum("running_avg_credit_limit") / F.nullif(F.count(F.when(F.col("city") == "Singapore", 1)), F.lit(0)), 2).alias("running_avg_credit_limit_per_singapore"),
        F.corr("credit_limit", "age").alias("corr_credit_limit_age"),
        F.round(F.sum("running_min_credit_limit") / F.nullif(F.count(F.when(F.col("country") == "JP", 1)), F.lit(0)), 2).alias("running_min_credit_limit_per_jp"),
        F.count_if(F.col("city") == "New York").alias("new_york_events"),
        F.grouping_id().alias("gid"),
    )
    .orderBy(F.col("running_min_credit_limit_per_jp").desc())
)
