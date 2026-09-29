from pyspark.sql import functions as F

recent_listeners = (
    df_listeners
    .filter(F.col("birth_year") < 1995)
    .select(
        "id",
        "username",
        "plan",
        "country",
        "signup_date",
        "birth_year",
        "interests",
        F.when(F.col("birth_year") >= 1980, "HIGH").when(F.col("birth_year") >= 1975, "MEDIUM").otherwise("LOW").alias("birth_year_tier"),
        F.when(F.col("country").isin("JP", "KR"), "group_a").when(F.col("country") == "TH", "group_b").otherwise("other").alias("country_group"),
        F.add_months(F.col("signup_date"), 1).alias("signup_date_plus_1m"),
    )
)

grouped = (
    recent_listeners
    .groupBy(
        "birth_year_tier",
        F.year("signup_date_plus_1m").alias("signup_date_plus_1m_year"),
        "plan",
    )
    .agg(
        F.sum("birth_year").alias("total_birth_year"),
        F.countDistinct("id").alias("distinct_ids"),
        F.datediff(F.max("signup_date"), F.min("signup_date")).alias("signup_date_span_days"),
        F.round(100 * F.count(F.when(F.col("username").isNotNull(), 1)) / F.count("*"), 1).alias("pct_username_is_not_null"),
        F.round(F.avg("birth_year"), 2).alias("avg_birth_year"),
    )
)

result = (
    grouped
    .groupBy("birth_year_tier")
    .pivot("plan", ["student", "premium", "free"])
    .agg(F.avg("signup_date_plus_1m_year"))
    .orderBy("birth_year_tier")
)
