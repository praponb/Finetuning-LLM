from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_cohort_group = Window.partitionBy("cohort_group")
w_cohort_group_major_by_gpa_fi = Window.partitionBy("cohort_group", "major").orderBy("gpa_filled")
w_cohort_group_major_by_gpa_fi_2 = Window.partitionBy("cohort_group", "major").orderBy("gpa_filled", "id")

prepared = (
    df_students
    .filter((F.length(F.trim(F.col("student_name"))) >= 5) | (F.col("gpa") > 2.7))
    .select(
        "id",
        "cohort",
        "major",
        "enrolled_on",
        F.coalesce(F.col("gpa"), F.lit(0.0)).alias("gpa_filled"),
        F.when(F.col("cohort").isin("C2021", "C2023"), "group_a").when(F.col("cohort") == "C2022", "group_b").otherwise("other").alias("cohort_group"),
        F.substring(F.trim(F.col("cohort")), 1, 4).alias("cohort_prefix"),
    )
)

sequenced = (
    prepared
    .select(
        "*",
        F.count(F.lit(1)).over(w_cohort_group).alias("rows_in_cohort_group"),
        F.rank().over(w_cohort_group_major_by_gpa_fi).alias("gpa_filled_rank"),
        F.dense_rank().over(w_cohort_group_major_by_gpa_fi_2).alias("gpa_filled_dense_rank"),
    )
)

qualified = (
    sequenced.alias("s")
    .join(df_enrollments.alias("e"), (F.col("e.student_id") == F.col("s.id")) & (F.col("e.term") != "2025T1"), "left_semi")
)

result = (
    qualified
    .filter(F.col("major").isin("Art", "CS"))
    .select(
        "id",
        "cohort",
        "major",
        "enrolled_on",
        "gpa_filled",
        "cohort_group",
        "cohort_prefix",
        "rows_in_cohort_group",
        "gpa_filled_rank",
        "gpa_filled_dense_rank",
        F.round(F.col("gpa_filled") * 1.5, 2).alias("gpa_filled_adj"),
        F.date_format(F.col("enrolled_on"), "MMM yyyy").alias("enrolled_on_label"),
    )
)
