from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("term").orderBy(F.col("enrolled_at_dayofweek").desc(), F.col("id").desc())

prepared = (
    df_enrollments
    .filter((F.col("course_id") >= 7) & (F.col("score") <= 57.8))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        F.dayofweek(F.col("enrolled_at")).alias("enrolled_at_dayofweek"),
        F.initcap(F.trim(F.col("grade_status"))).alias("grade_status_clean"),
        F.date_format(F.col("enrolled_at"), "MMM yyyy").alias("enrolled_at_label"),
    )
)

latest_per_key = (
    prepared
    .withColumn("row_num", F.row_number().over(w_latest))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

result = (
    latest_per_key
    .cube("course_id", "term")
    .agg(F.countDistinct("course_id").alias("distinct_course_ids"))
    .orderBy(F.col("course_id").asc_nulls_last(), F.col("term").asc_nulls_last())
)
