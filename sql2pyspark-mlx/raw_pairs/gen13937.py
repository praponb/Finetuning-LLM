from pyspark.sql import functions as F

valid_stores = (
    df_stores
    .filter(F.col("region").isNotNull())
    .withColumn("opened_date_month_end", F.last_day("opened_date"))
)

agg = (
    valid_stores
    .groupBy(
        F.when(F.year("opened_date_month_end") < 2024, "before_2024").when(F.year("opened_date_month_end") == 2024, "2024").otherwise("after_2024").alias("opened_month_end_cohort"),
    )
    .agg(
        F.round(F.avg("sqft"), 2).alias("avg_sqft"),
        F.round(((100 * F.sum(F.when(F.col("region") == "North", 1).otherwise(0))) / F.count("*")), 1).alias("pct_north"),
    )
)

result = (
    agg
    .select("opened_month_end_cohort", "avg_sqft", "pct_north")
    .orderBy(F.col("pct_north").desc(), "opened_month_end_cohort")
    .limit(3)
)
