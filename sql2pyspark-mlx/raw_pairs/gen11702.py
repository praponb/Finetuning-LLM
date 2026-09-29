from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy("age", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("country").orderBy("credit_limit", "id")
w3 = Window.partitionBy(F.month("signup_date"), "city").orderBy("credit_limit", "id").rowsBetween(-3, -1)
w4 = Window.partitionBy("segment").orderBy(F.col("credit_limit_avg_3_prior").asc_nulls_first(), "id")

cleaned = (
    df_customers
    .filter(F.col("signup_date") > F.lit("2023-04-11").cast("date"))
    .select(
        "*",
        F.concat_ws(",", "country", "email").alias("country_email_key"),
        F.upper("email").alias("email_upper"),
    )
)

enriched = (
    cleaned
    .filter(F.month("signup_date").isin([5, 6, 7]))
    .select(
        "id",
        "name",
        "city",
        "segment",
        "signup_date",
        "is_active",
        "credit_limit",
        "country_email_key",
        F.first("city", ignorenulls=True).over(w).alias("first_known_city"),
        F.ntile(3).over(w2).alias("credit_limit_bucket"),
        F.avg("credit_limit").over(w3).alias("credit_limit_avg_3_prior"),
    )
)

ranked = (
    enriched
    .withColumn("rank_in_group", F.row_number().over(w4))
    .filter(F.col("rank_in_group") == 1)
    .select(
        "id",
        "name",
        "country_email_key",
        "credit_limit_bucket",
        "credit_limit_avg_3_prior",
    )
)

eligible = (
    ranked
    .alias("s")
    .join(df_orders.alias("c"), (F.col("c.customer_id") == F.col("s.id")) & (F.col("c.shipping_fee") > 12), "left_anti")
)

result = (
    eligible
    .groupBy(
        F.when(F.col("credit_limit_avg_3_prior") >= 17980, "PREMIUM").otherwise("STANDARD").alias("credit_limit_avg_3_prior_tier"),
    )
    .agg(
        F.percentile_approx("credit_limit_avg_3_prior", 0.95).alias("credit_limit_avg_3_prior_p95"),
        F.count("*").alias("num_rows"),
        F.count("*").alias("event_count"),
        F.percentile_approx("credit_limit_bucket", [0.25, 0.5, 0.75]).alias("credit_limit_bucket_quartiles"),
    )
    .orderBy(F.col("credit_limit_avg_3_prior_p95").desc())
)
