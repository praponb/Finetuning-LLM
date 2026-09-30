from pyspark.sql import functions as F

recent = (
    df_students
    .filter(F.col("gpa") >= 2.1)
    .select(
        "id",
        "cohort",
        "major",
        "enrolled_on",
        "gpa",
        "is_international",
        F.when(F.col("is_international"), 1).otherwise(0).alias("is_international_flag"),
    )
)

annotated = (
    recent
    .select(
        "*",
        F.date_trunc("quarter", F.col("enrolled_on")).alias("enrolled_on_quarter"),
        F.substring(F.trim(F.col("major")), 1, 4).alias("major_prefix"),
    )
)

tagged = annotated.withColumn("cohort_or_unknown", F.coalesce(F.col("cohort"), F.lit("UNKNOWN")))

result = (
    tagged
    .select("cohort", "gpa", "major_prefix")
    .distinct()
    .orderBy("cohort", F.col("gpa").desc_nulls_first())
)
