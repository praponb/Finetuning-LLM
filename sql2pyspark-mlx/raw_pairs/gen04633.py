from pyspark.sql import functions as F

base = (
    df_customers
    .filter(F.col("signup_date") > F.lit("2023-08-09").cast("date"))
    .select(
        "id",
        "name",
        "email",
        "country",
        "signup_date",
        "is_active",
        "credit_limit",
        F.initcap(F.trim("city")).alias("city_title"),
        F.add_months("signup_date", 1).alias("signup_date_plus_months"),
    )
)

result = (
    base
    .cube("country")
    .agg(
        F.round(F.sum("credit_limit") / F.nullif(F.count(F.when(F.col("country") == "US", 1)), F.lit(0)), 2).alias("credit_limit_per_us"),
        F.percentile_approx("credit_limit", [0.25, 0.5, 0.75]).alias("credit_limit_quartiles"),
        F.count("*").alias("event_count"),
        F.sum(F.when(F.col("country") == "US", F.col("credit_limit")).otherwise(0)).alias("us_credit_limit"),
        F.grouping("country").alias("grouping_level"),
    )
)
