from pyspark.sql import functions as F

prepared = (
    df_students
    .filter(F.col("major") == "Art")
    .select(
        "id",
        "student_name",
        "cohort",
        "major",
        "gpa",
        "is_international",
        F.when(F.col("is_international"), 1).otherwise(0).alias("is_international_flag"),
    )
)

labeled = (
    prepared
    .select(
        "*",
        F.round(F.col("gpa") * 1.07, 2).alias("gpa_adj"),
        F.when(F.col("gpa") >= 3.4, "HIGH").when(F.col("gpa") >= 2.7, "MEDIUM").otherwise("LOW").alias("gpa_tier"),
        F.initcap(F.trim(F.col("student_name"))).alias("student_name_clean"),
    )
)

tagged = (
    labeled
    .filter(F.col("cohort") != "C2021")
    .select(
        "*",
        F.when(F.col("is_international"), 1).otherwise(0).alias("is_international_flag_2"),
        F.lower(F.regexp_replace(F.trim(F.col("student_name_clean")), " ", "_")).alias("student_name_clean_slug"),
    )
)

result = (
    tagged
    .groupingSets(
        [["major", "is_international", "cohort"], ["major"], ["is_international"], []],
        "major", "is_international", "cohort",
    )
    .agg(
        F.grouping_id().alias("grouping_level"),
        F.array_sort(F.collect_set("major")).alias("major_list"),
        F.array_sort(F.collect_set("cohort")).alias("cohort_list"),
    )
    .select(
        F.coalesce(F.col("major"), F.lit("ALL")).alias("major"),
        "is_international",
        F.coalesce(F.col("cohort"), F.lit("ALL")).alias("cohort"),
        "grouping_level",
        "major_list",
        "cohort_list",
    )
    .orderBy(
        F.col("major").asc_nulls_last(),
        F.col("is_international").asc_nulls_last(),
        F.col("cohort").asc_nulls_last(),
    )
)
