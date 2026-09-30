from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("grade_status_group", "term").orderBy(F.col("score_bucket").desc(), F.col("id").desc())

recent_enrollments = (
    df_enrollments
    .filter((F.col("score") >= 10.1) & (F.col("course_id").isNotNull()))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        F.lower(F.regexp_replace(F.trim(F.col("term")), " ", "_")).alias("term_slug"),
        F.when(F.col("grade_status").isin("PASS", "WITHDRAWN"), "group_a").when(F.col("grade_status") == "FAIL", "group_b").otherwise("other").alias("grade_status_group"),
        (F.floor(F.col("score") / 25) * 25).alias("score_bucket"),
    )
)

latest_per_key = (
    recent_enrollments
    .withColumn("row_num", F.row_number().over(w_dedup))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

result = (
    latest_per_key
    .groupBy("grade_status_group")
    .pivot("grade_status", ["WITHDRAWN", "FAIL"])
    .agg(F.sum("score_bucket"))
)
