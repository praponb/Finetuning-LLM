from pyspark.sql import functions as F

valid = (
    df_customers
    .filter(F.col("city").isNotNull() & (~F.col("city").eqNullSafe(F.lit("Tokyo"))))
    .withColumn("country_upper", F.upper("country"))
    .withColumn("city_clean", F.coalesce("city", F.lit("DIRECT")))
)

valid_customers = (
    valid
    .filter(F.col("city").isNotNull() & F.upper("name").like("%OAH%"))
    .withColumn("name_title", F.initcap(F.trim("name")))
)

kept = (
    valid_customers
    .alias("s")
    .join(df_accounts.alias("ch"), (F.col("ch.customer_id") == F.col("s.id")) & (~F.col("ch.status").isin(["CLOSED", "OPEN"])), "left_anti")
)

cleaned = (
    kept
    .filter(F.col("signup_date") > F.lit("2023-03-04").cast("date"))
    .select(
        "id",
        "name",
        "email",
        "country",
        "city",
        "segment",
        "signup_date",
        "is_active",
        "country_upper",
        "city_clean",
        "name_title",
        (F.col("age") * F.col("credit_limit")).alias("age_times_credit_limit"),
        F.month("signup_date").alias("signup_date_month"),
        F.coalesce("city", F.lit("OTHER")).alias("city_clean_2"),
    )
)

cleaned_customers = (
    cleaned
    .filter(F.col("signup_date") > F.to_date(F.lit("2023-02-05")))
    .select(
        "id",
        "country",
        "city",
        "segment",
        "signup_date",
        "is_active",
        "country_upper",
        "name_title",
        "age_times_credit_limit",
        "signup_date_month",
        F.round("age_times_credit_limit", 1).alias("age_times_credit_limit_rounded"),
        F.col("is_active").cast("int").alias("is_active_flag"),
    )
)

result = (
    cleaned_customers
    .withColumn("activity_month", F.date_trunc("month", "signup_date"))
    .rollup("activity_month")
    .agg(
        F.round(F.avg("is_active_flag"), 2).alias("avg_is_active_flag"),
        F.count("*").alias("row_count"),
        F.count(F.when(F.col("city") == "Bangkok", 1)).alias("bangkok_count"),
        F.grouping_id().alias("grouping_id"),
    )
    .orderBy(F.col("activity_month").desc())
)
