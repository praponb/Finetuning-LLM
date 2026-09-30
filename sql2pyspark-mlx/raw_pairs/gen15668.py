from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("credit_limit").desc(), "id")
w2 = Window.partitionBy("city", "segment")
w3 = Window.partitionBy("country", "city").orderBy(F.col("credit_limit").desc())
w4 = Window.partitionBy("country").orderBy(F.col("signup_date").desc(), "id")
w5 = Window.partitionBy("city", "segment").orderBy("dedup_rank", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_customers
    .filter((F.col("signup_date") >= F.lit("2023-07-30").cast("date")) & (F.col("age") != 65))
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
        F.lower("segment").alias("segment_lower"),
        F.last_day("signup_date").alias("signup_date_month_end"),
    )
)

enriched = (
    base
    .select(
        "*",
        F.ntile(10).over(w).alias("credit_limit_bucket"),
        F.count("credit_limit").over(w2).alias("city_count_credit_limit"),
    )
)

latest = (
    enriched
    .withColumn("dedup_rank", F.rank().over(w3))
    .filter(F.col("dedup_rank") == 1)
    .drop("dedup_rank")
)

ranked = (
    latest
    .withColumn("dedup_rank", F.row_number().over(w4))
    .filter(F.col("dedup_rank") == 1)
)

enriched_customers = (
    ranked
    .select(
        "id",
        "email",
        "credit_limit",
        "segment_lower",
        "credit_limit_bucket",
        "dedup_rank",
        F.count("city_count_credit_limit").over(w5).alias("running_count_city_count_credit_limit"),
        F.sum("city_count_credit_limit").over(w5).alias("running_sum_city_count_credit_limit"),
        F.avg("city_count_credit_limit").over(w5).alias("running_avg_city_count_credit_limit"),
    )
)

result = (
    enriched_customers
    .select(
        "id",
        "email",
        "dedup_rank",
        "running_sum_city_count_credit_limit",
        "running_avg_city_count_credit_limit",
    )
    .orderBy(F.col("running_avg_city_count_credit_limit").desc_nulls_first())
)
