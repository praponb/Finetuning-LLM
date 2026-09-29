from pyspark.sql import functions as F

cleaned = df_courses

screened = (
    cleaned
    .crossJoin(cleaned.agg(F.max("credits").alias("overall_max_credits")))
    .filter(F.col("credits") < F.col("overall_max_credits"))
    .drop("overall_max_credits")
)

result = (
    screened
    .cube("dept", "level")
    .agg(F.countDistinct("id").alias("distinct_ids"))
    .orderBy(F.col("dept").asc_nulls_last(), F.col("level").asc_nulls_last())
)
