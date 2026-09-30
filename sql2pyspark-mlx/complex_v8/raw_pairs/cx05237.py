from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("country").orderBy(F.col("signup_date").desc(), F.col("id").desc())

base = (
    df_listeners
    .filter(F.col("plan").isin("premium", "student", "free"))
    .select(
        "id",
        "username",
        "country",
        "signup_date",
        F.upper(F.trim(F.col("plan"))).alias("plan_clean"),
    )
)

unique_rows = (
    base
    .withColumn("recency_rank", F.row_number().over(w_dedup))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

grouped = (
    unique_rows
    .groupBy(F.year("signup_date").alias("signup_date_year"), "country")
    .agg(
        F.count(F.when(~F.col("country").isin("KR", "UK"), 1)).alias("n_country_not_in_kr_uk"),
        F.max("signup_date").alias("last_signup_date"),
        F.count("*").alias("cnt"),
    )
)

scored = (
    grouped
    .select(
        "*",
        F.when(F.col("signup_date_year") >= 2025, "large").when(F.col("signup_date_year") >= 2024, "medium").otherwise("small").alias("signup_date_year_tier"),
    )
)

qualified = (
    scored
    .crossJoin(scored.agg(F.avg("cnt").alias("overall_avg_cnt")))
    .filter(F.col("cnt") > F.col("overall_avg_cnt"))
    .drop("overall_avg_cnt")
)

derived = (
    qualified
    .select(
        "signup_date_year",
        "country",
        "cnt",
        "signup_date_year_tier",
        F.greatest(F.col("signup_date_year"), F.col("cnt")).alias("greatest_signup_date_year_cnt"),
        F.add_months(F.col("last_signup_date"), 12).alias("last_signup_date_plus_12m"),
    )
)

result = (
    derived
    .filter(F.col("signup_date_year") < 2025)
    .select(
        "signup_date_year",
        "country",
        "cnt",
        "signup_date_year_tier",
        "last_signup_date_plus_12m",
    )
    .orderBy("signup_date_year_tier", F.col("country").desc())
)
