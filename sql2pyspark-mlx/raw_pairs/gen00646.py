from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy(
        F.when(F.year("opened_date") < 2024, "before_2024").when(F.year("opened_date") == 2024, "2024").otherwise("after_2024").alias("opened_cohort"),
    )
    .agg(
        F.round(((100 * F.sum(F.when(F.col("region") == "South", 1).otherwise(0))) / F.count("*")), 1).alias("pct_south"),
    )
    .orderBy("opened_cohort")
)
