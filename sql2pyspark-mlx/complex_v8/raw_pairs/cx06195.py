from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_student_name_len_is_internat = Window.partitionBy("student_name_len", "is_international")
w_student_name_len_by_student = Window.partitionBy("student_name_len").orderBy("student_name_len", "id").rowsBetween(-1, -1)
w_all_by_gpa_full = Window.orderBy(F.col("gpa").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

scoped = (
    df_students
    .filter((F.col("is_international")) & (F.col("cohort") == "C2023"))
    .select(
        "id",
        "student_name",
        "enrolled_on",
        "gpa",
        "is_international",
        F.round(F.col("gpa") * 1.5, 2).alias("gpa_adj"),
        F.length(F.trim(F.col("student_name"))).alias("student_name_len"),
    )
)

with_trends = (
    scoped
    .select(
        "id",
        "student_name",
        "enrolled_on",
        "gpa",
        "is_international",
        "student_name_len",
        F.round(F.col("student_name_len") / F.nullif(F.sum("student_name_len").over(w_student_name_len_is_internat), F.lit(0)), 4).alias("student_name_len_share_of_student_name_len_is_in"),
        F.max("gpa").over(w_student_name_len_is_internat).alias("student_name_len_is_international_max_gpa"),
        F.max("gpa_adj").over(w_student_name_len_by_student).alias("gpa_adj_max_1_prior"),
        F.nth_value("gpa", 2).over(w_all_by_gpa_full).alias("second_gpa"),
        F.round(F.col("gpa") - F.avg("gpa").over(w_student_name_len_is_internat), 2).alias("gpa_vs_student_name_len_is_international_avg"),
    )
)

filtered = (
    with_trends.alias("s")
    .join(df_enrollments.alias("e"), (F.col("e.student_id") == F.col("s.id")) & (F.col("e.term") == "2025T2"), "left_semi")
)

result = (
    filtered
    .filter(F.col("student_name_len") < 10)
    .select(
        "id",
        "enrolled_on",
        "gpa",
        "student_name_len",
        "student_name_len_is_international_max_gpa",
        "second_gpa",
        "gpa_vs_student_name_len_is_international_avg",
        F.when(F.col("is_international"), "yes").otherwise("no").alias("is_international_label"),
        F.greatest(F.col("second_gpa"), F.col("student_name_len")).alias("greatest_second_gpa_student_name_len"),
    )
    .orderBy(
        F.col("id").desc(),
        F.col("student_name_len_is_international_max_gpa").desc(),
    )
)
