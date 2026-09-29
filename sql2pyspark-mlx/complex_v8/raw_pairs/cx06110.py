from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_course_dept_by_score = Window.partitionBy("course_dept").orderBy(F.col("score").desc(), "id")
w_course_level_by_score = Window.partitionBy("course_level").orderBy("score", "id")
w_course_dept_by_score_full = Window.partitionBy("course_dept").orderBy(F.col("score").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_student_id_by_score_full = Window.partitionBy("student_id").orderBy(F.col("score").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_student_id_by_score_ties = Window.partitionBy("student_id").orderBy(F.col("score").desc_nulls_first())
w_student_id_course_dept = Window.partitionBy("student_id", "course_dept")
w_student_id = Window.partitionBy("student_id")

base = (
    df_enrollments
    .filter(F.col("student_id") <= 12)
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        F.quarter(F.col("enrolled_at")).alias("enrolled_at_quarter"),
        F.when(F.col("score") >= 56.1, "A").when(F.col("score") >= 35.8, "B").otherwise("C").alias("score_tier"),
    )
)

enriched_course = (
    base.alias("s")
    .join(df_courses.alias("c"), F.col("c.id") == F.col("s.course_id"), "left")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.term",
        "s.score",
        "s.enrolled_at_quarter",
        F.col("c.level").alias("course_level"),
        F.col("c.dept").alias("course_dept"),
    )
)

with_windows = (
    enriched_course
    .select(
        "*",
        F.ntile(5).over(w_course_dept_by_score).alias("score_quintile"),
        (F.col("score") - F.lag("score").over(w_course_level_by_score)).alias("score_change"),
        F.last("enrolled_at_quarter").over(w_course_dept_by_score_full).alias("last_enrolled_at_quarter"),
        F.nth_value("enrolled_at_quarter", 2).over(w_student_id_by_score_full).alias("second_enrolled_at_quarter"),
        F.round(F.percent_rank().over(w_student_id_by_score_ties), 3).alias("score_percent_rank"),
        F.round(F.col("score") / F.nullif(F.sum("score").over(w_student_id_course_dept), F.lit(0)), 4).alias("score_share_of_student_id_course_dept"),
        F.sum("score").over(w_student_id_course_dept).alias("student_id_course_dept_sum_score"),
        F.lead("score").over(w_course_level_by_score).alias("next_score"),
        (F.col("score") - F.lag("score").over(w_course_dept_by_score)).alias("score_change_2"),
        F.round(F.col("enrolled_at_quarter") - F.avg("enrolled_at_quarter").over(w_student_id_course_dept), 2).alias("enrolled_at_quarter_vs_student_id_course_dept_av"),
        F.round(F.col("score") / F.nullif(F.sum("score").over(w_student_id), F.lit(0)), 4).alias("score_share_of_student_id"),
        F.count(F.lit(1)).over(w_student_id_course_dept).alias("rows_in_student_id_course_dept"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "enrolled_at_quarter",
        "score_change",
        "last_enrolled_at_quarter",
        "second_enrolled_at_quarter",
        "score_percent_rank",
        "next_score",
        "score_change_2",
        "enrolled_at_quarter_vs_student_id_course_dept_av",
        "score_share_of_student_id",
        "rows_in_student_id_course_dept",
    )
    .orderBy(F.col("term").desc())
)
