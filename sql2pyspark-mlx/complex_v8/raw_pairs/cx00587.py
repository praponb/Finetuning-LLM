from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_course_id_grade_status_by_en = Window.partitionBy("course_id", "grade_status").orderBy("enrolled_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_student_id_grade_status_by_e = Window.partitionBy("student_id", "grade_status").orderBy(F.col("enrolled_at").desc(), "id")
w_grade_status_student_id = Window.partitionBy("grade_status", "student_id")
w_course_id_grade_status_by_sc = Window.partitionBy("course_id", "grade_status").orderBy("score", "id")
w_course_id_grade_status_by_en_2 = Window.partitionBy("course_id", "grade_status").orderBy("enrolled_at")
w_course_id_grade_status_by_en_3 = Window.partitionBy("course_id", "grade_status").orderBy("enrolled_at", "id")

cleaned_enrollments = (
    df_enrollments
    .filter((F.col("score") > 17.4) & (F.col("course_id").isNotNull()))
    .select(
        "id",
        "student_id",
        "course_id",
        "score",
        "grade_status",
        "enrolled_at",
        F.concat_ws(": ", F.col("term"), F.col("grade_status")).alias("term_grade_status_label"),
        F.lower(F.regexp_replace(F.trim(F.col("grade_status")), " ", "_")).alias("grade_status_slug"),
    )
)

ranked = (
    cleaned_enrollments
    .select(
        "id",
        "course_id",
        "score",
        "grade_status",
        "enrolled_at",
        F.first("enrolled_at", ignorenulls=True).over(w_course_id_grade_status_by_en).alias("first_known_enrolled_at"),
        F.row_number().over(w_student_id_grade_status_by_e).alias("enrolled_at_seq"),
        F.round(F.col("score") - F.avg("score").over(w_grade_status_student_id), 2).alias("score_vs_grade_status_student_id_avg"),
        F.size(F.collect_set("course_id").over(w_grade_status_student_id)).alias("distinct_course_id_in_grade_status_student_id"),
        F.round(F.cume_dist().over(w_course_id_grade_status_by_sc), 3).alias("score_cume_dist"),
        F.count(F.lit(1)).over(w_grade_status_student_id).alias("rows_in_grade_status_student_id"),
        F.lead("score", 2).over(w_course_id_grade_status_by_sc).alias("next2_score"),
        F.round(F.col("score") / F.nullif(F.sum("score").over(w_grade_status_student_id), F.lit(0)), 4).alias("score_share_of_grade_status_student_id"),
        F.round(F.percent_rank().over(w_course_id_grade_status_by_en_2), 3).alias("enrolled_at_percent_rank"),
        F.lag("score").over(w_course_id_grade_status_by_en_3).alias("prev_score"),
    )
)

tagged = ranked.withColumn("prev_score_adj", F.round(F.col("prev_score") * 1.5, 2))

result = (
    tagged
    .select(
        "id",
        "course_id",
        "score",
        "grade_status",
        "enrolled_at",
        "first_known_enrolled_at",
        "score_vs_grade_status_student_id_avg",
        "distinct_course_id_in_grade_status_student_id",
        "rows_in_grade_status_student_id",
        "next2_score",
        "score_share_of_grade_status_student_id",
        "enrolled_at_percent_rank",
        "prev_score",
        "prev_score_adj",
    )
)
