from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("credit_limit").between(5000, 13400) & F.month("signup_date").isin([2, 3, 6]) & (F.col("signup_date") >= "2025-01-17"))
    .groupBy(
        F.date_format("signup_date", "yyyy-MM").alias("signup_month"),
        "segment",
        "city",
    )
    .agg(
        F.sort_array(F.collect_set("segment")).alias("segment_list"),
        F.sort_array(F.collect_set("country")).alias("country_list"),
    )
    .orderBy(F.col("city").desc())
)
