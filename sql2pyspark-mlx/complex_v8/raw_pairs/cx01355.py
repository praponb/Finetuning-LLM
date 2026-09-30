from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_student_is_international_by = Window.partitionBy("student_is_international").orderBy(F.col("student_gpa").desc(), "id")
w_student_is_international_by_2 = Window.partitionBy("student_is_international").orderBy(F.col("student_gpa").desc())
w_student_id_student_is_intern = Window.partitionBy("student_id", "student_is_international")
w_student_is_international_stu = Window.partitionBy("student_is_international", "student_gpa_rank").orderBy("student_gpa_cume_dist", "id")

covered = (
    df_enrollments.alias("a")
    .join(
        df_enrollments.alias("b"),
        (F.col("b.student_id") == F.col("a.student_id"))
        & (F.col("b.enrolled_at") < F.col("a.enrolled_at"))
        & (F.col("b.enrolled_at") >= F.col("a.enrolled_at") - F.expr("INTERVAL 7 DAYS")),
        "left",
    )
    .groupBy("a.id", "a.student_id", "a.enrolled_at")
    .agg(
        F.count("b.id").alias("prior_rows_7d"),
        F.sum("b.score").alias("prior_score_7d"),
    )
)

enriched = (
    covered.alias("s")
    .join(df_students.alias("st"), F.col("st.id") == F.col("s.student_id"), "inner")
    .filter(F.col("st.gpa") < 3.3)
    .select(
        "s.id",
        "s.student_id",
        "s.prior_score_7d",
        F.col("st.is_international").alias("student_is_international"),
        F.col("st.gpa").alias("student_gpa"),
    )
)

with_windows = (
    enriched
    .select(
        "*",
        F.round(F.cume_dist().over(w_student_is_international_by), 3).alias("student_gpa_cume_dist"),
        F.rank().over(w_student_is_international_by_2).alias("student_gpa_rank"),
    )
)

sequenced = (
    with_windows
    .select(
        "*",
        F.round(F.col("prior_score_7d") - F.avg("prior_score_7d").over(w_student_id_student_is_intern), 2).alias("prior_score_7d_vs_student_id_student_is_internat"),
        F.ntile(10).over(w_student_is_international_stu).alias("student_gpa_cume_dist_decile"),
    )
)

result = (
    sequenced
    .cube("student_id", "student_is_international")
    .agg(
        F.grouping("student_id").alias("is_total_student_id"),
        F.min("prior_score_7d").alias("min_prior_score_7d"),
        F.sum(F.when(~F.col("student_is_international"), F.col("student_gpa_cume_dist_decile"))).alias("student_gpa_cume_dist_decile_student_is_internat"),
        F.round(F.avg("student_gpa_cume_dist"), 2).alias("avg_student_gpa_cume_dist"),
    )
    .orderBy(
        F.col("student_id").asc_nulls_last(),
        F.col("student_is_international").asc_nulls_last(),
    )
)
