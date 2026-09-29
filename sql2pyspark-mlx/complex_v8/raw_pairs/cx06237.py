from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grade_status_term_by_score_m = Window.partitionBy("grade_status", "term").orderBy(F.col("score").asc_nulls_last(), "id").rowsBetween(-2, Window.currentRow)
w_all_by_score = Window.orderBy("score", "id")
w_term_by_score_running = Window.partitionBy("term").orderBy(F.col("score").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_term_by_score_full = Window.partitionBy("term").orderBy(F.col("score").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_term_by_score = Window.partitionBy("term").orderBy(F.col("score").desc(), "id")
w_student_id = Window.partitionBy("student_id")
w_all_by_score_2 = Window.orderBy(F.col("score").asc_nulls_last(), "id")
w_grade_status_by_score = Window.partitionBy("grade_status").orderBy("score", "id")
w_all_by_first_known_score_tie = Window.orderBy("first_known_score")
w_score_tercile_grade_status_b = Window.partitionBy("score_tercile", "grade_status").orderBy(F.col("second_student_enrolled_on").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_grade_status_by_student_enro = Window.partitionBy("grade_status").orderBy("student_enrolled_on", "id")
w_all_by_first_known_score_mov = Window.orderBy("first_known_score", "id").rowsBetween(-6, Window.currentRow)
w_grade_status_by_student_enro_2 = Window.partitionBy("grade_status").orderBy("student_enrolled_on", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_score_tercile_term = Window.partitionBy("score_tercile", "term")

cleaned = (
    df_enrollments
    .filter(F.col("term") == "2025T2")
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
    )
)

joined = (
    cleaned.alias("s")
    .join(df_students.alias("st"), F.col("st.id") == F.col("s.student_id"), "left")
    .select("s.*", F.col("st.enrolled_on").alias("student_enrolled_on"))
)

windowed = (
    joined
    .select(
        "*",
        F.sum("score").over(w_grade_status_term_by_score_m).alias("score_sum_3"),
        F.round(F.cume_dist().over(w_all_by_score), 3).alias("score_cume_dist"),
        F.first("score", ignorenulls=True).over(w_term_by_score_running).alias("first_known_score"),
        F.nth_value("student_enrolled_on", 2).over(w_term_by_score_full).alias("second_student_enrolled_on"),
        F.dense_rank().over(w_term_by_score).alias("score_dense_rank"),
        F.sum("score").over(w_student_id).alias("student_id_sum_score"),
        F.ntile(3).over(w_all_by_score_2).alias("score_tercile"),
        (F.col("score") - F.lag("score").over(w_grade_status_by_score)).alias("score_change"),
    )
)

derived = (
    windowed
    .select(
        "id",
        "term",
        "grade_status",
        "student_enrolled_on",
        "first_known_score",
        "second_student_enrolled_on",
        "score_tercile",
        "score_change",
        F.concat(F.col("grade_status"), F.lit("#"), F.col("id").cast("string")).alias("grade_status_ref"),
        F.upper(F.trim(F.col("grade_status"))).alias("grade_status_clean"),
        (F.col("score_tercile") + F.col("first_known_score")).alias("score_tercile_plus_first_known_score"),
    )
)

metrics = (
    derived
    .select(
        "*",
        F.round(F.percent_rank().over(w_all_by_first_known_score_tie), 3).alias("first_known_score_percent_rank"),
        F.sum("score_change").over(w_score_tercile_grade_status_b).alias("running_score_change"),
        F.dense_rank().over(w_grade_status_by_student_enro).alias("student_enrolled_on_dense_rank"),
        F.round(F.avg("first_known_score").over(w_all_by_first_known_score_mov), 2).alias("first_known_score_avg_7"),
        F.lag("score_change").over(w_grade_status_by_student_enro).alias("prev_score_change"),
        F.first("second_student_enrolled_on", ignorenulls=True).over(w_grade_status_by_student_enro_2).alias("first_known_second_student_enrolled_on"),
        F.max("score_tercile").over(w_score_tercile_term).alias("score_tercile_term_max_score_tercile"),
        F.count(F.lit(1)).over(w_score_tercile_term).alias("rows_in_score_tercile_term"),
    )
)

result = (
    metrics
    .select(
        "id",
        "term",
        "grade_status",
        "first_known_score",
        "second_student_enrolled_on",
        "score_change",
        "grade_status_clean",
        "score_tercile_plus_first_known_score",
        "first_known_score_percent_rank",
        "student_enrolled_on_dense_rank",
        "first_known_score_avg_7",
        "first_known_second_student_enrolled_on",
        F.add_months(F.col("student_enrolled_on"), 12).alias("student_enrolled_on_plus_12m"),
        (F.floor(F.col("running_score_change") / 250) * 250).alias("running_score_change_bucket"),
    )
    .orderBy(
        "student_enrolled_on_plus_12m",
        F.col("second_student_enrolled_on").desc(),
    )
)
