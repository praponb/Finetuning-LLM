from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy("days_since_signup_date_whole")
w2 = Window.partitionBy("country", "city").orderBy("signup_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_running = Window.orderBy(F.col("days_since_signup_date_whole").desc(), "id")
w_grp = Window.partitionBy("city").orderBy(F.col("days_since_signup_date_whole").desc(), "id")

filtered = (
    df_customers
    .filter((F.col("signup_date") >= F.lit("2023-09-28").cast("date")) & F.col("country").isin(["UK", "JP"]))
    .select(
        "*",
        F.datediff(F.current_date(), "signup_date").alias("days_since_signup_date"),
        F.coalesce("city", F.lit("N/A")).alias("resolved_city"),
    )
)

valid = (
    filtered
    .filter((F.col("signup_date") >= F.lit("2023-11-16").cast("date")) & F.col("segment").isNotNull())
    .select(
        "id",
        "name",
        "email",
        "country",
        "city",
        "signup_date",
        "is_active",
        "credit_limit",
        "resolved_city",
        F.when(F.col("credit_limit") >= 15100, "High").when(F.col("credit_limit") >= 4300, "Medium").otherwise("Low").alias("credit_limit_band"),
        F.floor("days_since_signup_date").alias("days_since_signup_date_whole"),
    )
)

eligible = (
    valid
    .alias("s")
    .join(df_subscriptions.alias("x"), (F.col("x.customer_id") == F.col("s.id")) & (F.col("x.end_date").between("2023-08-02", "2024-10-01")), "left_semi")
)

filtered_customers = (
    eligible
    .filter((F.col("signup_date") >= F.lit("2023-03-24").cast("date")) & F.col("city").isNotNull())
    .select(
        "*",
        F.when(F.col("days_since_signup_date_whole") >= 74, "High").when(F.col("days_since_signup_date_whole") >= 22, "Medium").otherwise("Low").alias("days_since_signup_date_whole_band"),
    )
)

enriched = (
    filtered_customers
    .withColumn("days_since_signup_date_whole_percent_rank", F.percent_rank().over(w))
    .withColumn("running_days_since_signup_date_whole", F.sum("days_since_signup_date_whole").over(w2))
    .withColumn("days_since_signup_date_whole_bucket", F.ntile(10).over(w_running))
    .withColumn("days_since_signup_date_whole_bucket_2", F.ntile(5).over(w_grp))
)

result = (
    enriched
    .select(
        "id",
        "name",
        "email",
        "country",
        "city",
        "is_active",
        "credit_limit",
        "days_since_signup_date_whole",
        "days_since_signup_date_whole_percent_rank",
    )
    .orderBy(F.col("id").desc_nulls_first(), "country")
)
