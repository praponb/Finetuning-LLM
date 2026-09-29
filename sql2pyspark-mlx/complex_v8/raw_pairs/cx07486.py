from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("student_id", "course_id").orderBy("enrolled_at", "id")

filtered = (
    df_enrollments
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "enrolled_at",
        F.date_format(F.col("enrolled_at"), "yyyy-MM-dd").alias("enrolled_at_label"),
        F.concat(F.col("grade_status"), F.lit("#"), F.col("course_id").cast("string")).alias("grade_status_ref"),
        F.concat_ws("-", F.col("term"), F.col("grade_status")).alias("term_grade_status_label"),
    )
)

latest_per_key = (
    filtered
    .withColumn("row_num", F.row_number().over(w_dedup))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

result = (
    latest_per_key
    .cube("term", "student_id")
    .agg(
        F.grouping_id().alias("grouping_level"),
        F.array_sort(F.collect_set("term")).alias("term_list"),
        F.sum("score").alias("total_score"),
        F.round(100 * F.count(F.when(F.col("term").isNotNull(), 1)) / F.count("*"), 1).alias("pct_term_is_not_null"),
    )
    .orderBy(F.col("term").asc_nulls_last(), F.col("student_id").asc_nulls_last())
)
