from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("cohort_or_unknown", "major").orderBy(F.col("enrolled_on_quarter").desc(), F.col("id").desc())

cleaned_students = (
    df_students
    .filter((~F.col("is_international")) & (F.col("major") == "Biz"))
    .select(
        "id",
        "student_name",
        "major",
        "gpa",
        "is_international",
        F.coalesce(F.col("cohort"), F.lit("UNKNOWN")).alias("cohort_or_unknown"),
        F.quarter(F.col("enrolled_on")).alias("enrolled_on_quarter"),
        F.upper(F.trim(F.col("student_name"))).alias("student_name_clean"),
    )
)

latest_per_key = (
    cleaned_students
    .withColumn("row_num", F.row_number().over(w_latest))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

result = (
    latest_per_key
    .rollup("cohort_or_unknown", "is_international", "enrolled_on_quarter")
    .agg(
        F.grouping_id().alias("gid"),
        F.array_sort(F.collect_set("student_name")).alias("student_name_list"),
    )
    .orderBy(
        F.col("cohort_or_unknown").asc_nulls_last(),
        F.col("is_international").asc_nulls_last(),
        F.col("enrolled_on_quarter").asc_nulls_last(),
    )
)
