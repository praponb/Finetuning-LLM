from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.partitionBy("segment").orderBy("credit_limit", "id")
w2 = Window.partitionBy("segment").orderBy("age", "id")

valid = (
    df_customers
    .filter(F.col("city").isNotNull())
    .select(
        "id",
        "name",
        "email",
        "city",
        "segment",
        "signup_date",
        "is_active",
        "credit_limit",
        "age",
        F.upper("city").alias("city_upper"),
    )
)

base = (
    valid
    .filter((F.col("signup_date") > F.to_date(F.lit("2023-04-19"))) & F.col("city").isNotNull())
    .select(
        "id",
        "name",
        "email",
        "segment",
        "is_active",
        "credit_limit",
        "age",
        "city_upper",
        F.length("email").alias("email_length"),
        F.trim("name").alias("clean_name"),
        F.coalesce("city", F.lit("N/A")).alias("city_filled"),
    )
)

eligible = (
    base
    .alias("s")
    .join(df_subscriptions.alias("ch"), (F.col("ch.customer_id") == F.col("s.id")) & (F.col("ch.monthly_fee") <= 210), "left_anti")
)

metrics = (
    eligible
    .filter(F.lower("email").like("%lic%"))
    .withColumn("credit_limit_bucket", F.ntile(3).over(w_recent))
    .withColumn("event_seq", F.row_number().over(w2))
)

result = (
    metrics
    .groupBy(
        "segment",
        F.when(F.col("age") >= 66, "LARGE").otherwise("SMALL").alias("age_tier"),
    )
    .agg(
        F.count("*").alias("cnt"),
        F.round(F.avg("credit_limit_bucket"), 2).alias("avg_credit_limit_bucket"),
    )
    .filter(F.col("avg_credit_limit_bucket") >= 0)
)
