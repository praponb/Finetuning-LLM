from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grade_status_by_score = Window.partitionBy("grade_status").orderBy("score", "id")
w_course_dept_student_id = Window.partitionBy("course_dept", "student_id")
w_course_id_course_dept_by_enr = Window.partitionBy("course_id", "course_dept").orderBy(F.col("enrolled_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_enrolled_at_moving_6 = Window.orderBy(F.col("enrolled_at").desc(), "id").rowsBetween(-6, Window.currentRow)
w_course_id = Window.partitionBy("course_id")
w_all_by_enrolled_at = Window.orderBy(F.col("enrolled_at").desc(), "id")
w_grade_status_by_score_runnin = Window.partitionBy("grade_status").orderBy("score", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_course_id_course_dept_by_enr_2 = Window.partitionBy("course_id", "course_dept").orderBy(F.col("enrolled_at").desc(), "id")
w_distinct_course_dept_in_cour = Window.partitionBy("distinct_course_dept_in_course_id", "rn")
w_all_by_score_vs_course_dept = Window.orderBy("score_vs_course_dept_student_id_avg", "id")

prepared_enrollments = (
    df_enrollments
    .filter((F.col("course_id") <= 11) | (~F.col("grade_status").isin("FAIL", "INCOMPLETE")))
    .select(
        "id",
        "student_id",
        "course_id",
        "score",
        "grade_status",
        "enrolled_at",
        F.concat(F.col("term"), F.lit("#"), F.col("course_id").cast("string")).alias("term_ref"),
    )
)

with_details = (
    prepared_enrollments.alias("s")
    .join(df_courses.alias("c"), F.col("c.id") == F.col("s.course_id"), "inner")
    .filter(~F.col("c.dept").isin("Bio", "Biz"))
    .select("s.*", "c.course_code", F.col("c.dept").alias("course_dept"))
)

ranked = (
    with_details
    .select(
        "id",
        "student_id",
        "course_id",
        "score",
        "grade_status",
        "enrolled_at",
        "term_ref",
        "course_code",
        "course_dept",
        F.row_number().over(w_grade_status_by_score).alias("seq_no"),
        F.round(F.col("score") - F.avg("score").over(w_course_dept_student_id), 2).alias("score_vs_course_dept_student_id_avg"),
        F.count(F.lit(1)).over(w_course_id_course_dept_by_enr).alias("running_count"),
        F.round(F.avg("score").over(w_all_by_enrolled_at_moving_6), 2).alias("score_avg_7"),
        F.count(F.lit(1)).over(w_course_dept_student_id).alias("rows_in_course_dept_student_id"),
        F.size(F.collect_set("course_dept").over(w_course_id)).alias("distinct_course_dept_in_course_id"),
        F.round(F.col("score") / F.nullif(F.sum("score").over(w_course_dept_student_id), F.lit(0)), 4).alias("score_share_of_course_dept_student_id"),
        F.min("score").over(w_course_dept_student_id).alias("course_dept_student_id_min_score"),
        F.ntile(4).over(w_all_by_enrolled_at).alias("enrolled_at_quartile"),
        F.first("enrolled_at", ignorenulls=True).over(w_grade_status_by_score_runnin).alias("first_known_enrolled_at"),
        F.max("score").over(w_course_id).alias("course_id_max_score"),
        F.round(F.col("score") - F.avg("score").over(w_course_id), 2).alias("score_vs_course_id_avg"),
        F.row_number().over(w_course_id_course_dept_by_enr_2).alias("rn"),
    )
)

sequenced = (
    ranked
    .select(
        "*",
        F.sum("seq_no").over(w_distinct_course_dept_in_cour).alias("distinct_course_dept_in_course_id_rn_sum_seq_no"),
        F.size(F.collect_set("grade_status").over(w_distinct_course_dept_in_cour)).alias("distinct_grade_status_in_distinct_course_dept_in"),
        F.size(F.collect_set("student_id").over(w_distinct_course_dept_in_cour)).alias("distinct_student_id_in_distinct_course_dept_in_c"),
        F.ntile(3).over(w_all_by_score_vs_course_dept).alias("score_vs_course_dept_student_id_avg_tercile"),
    )
)

result = (
    sequenced
    .select(
        "id",
        "student_id",
        "score",
        "grade_status",
        "enrolled_at",
        "term_ref",
        "course_code",
        "course_dept",
        "seq_no",
        "score_vs_course_dept_student_id_avg",
        "running_count",
        "score_avg_7",
        "rows_in_course_dept_student_id",
        "score_share_of_course_dept_student_id",
        "course_dept_student_id_min_score",
        "enrolled_at_quartile",
        "course_id_max_score",
        "rn",
        "distinct_course_dept_in_course_id_rn_sum_seq_no",
        "distinct_grade_status_in_distinct_course_dept_in",
        "distinct_student_id_in_distinct_course_dept_in_c",
        "score_vs_course_dept_student_id_avg_tercile",
    )
    .orderBy(
        F.col("distinct_course_dept_in_course_id_rn_sum_seq_no").asc(),
        F.col("course_dept_student_id_min_score").desc(),
    )
)
