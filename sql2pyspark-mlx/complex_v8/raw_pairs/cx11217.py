from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_major_student_name_len = Window.partitionBy("major", "student_name_len")
w_all_by_enrolled_on_ties = Window.orderBy("enrolled_on")
w_all_by_student_name_len_prio = Window.orderBy("student_name_len", "id").rowsBetween(-2, -1)
w_major_is_international = Window.partitionBy("major", "is_international")
w_cohort = Window.partitionBy("cohort")
w_major_cohort_by_gpa_distance = Window.partitionBy("major", "cohort").orderBy(F.col("gpa_distance").desc(), "id")
w_distinct_major_in_cohort_by = Window.partitionBy("distinct_major_in_cohort").orderBy(F.col("gpa").desc_nulls_first())
w_distinct_major_in_cohort_by_2 = Window.partitionBy("distinct_major_in_cohort").orderBy(F.col("gpa").desc_nulls_first(), "id")
w_student_name_len_by_gpa_dist = Window.partitionBy("student_name_len").orderBy("gpa_distance_vs_major_student_name_len_avg", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_major_cohort_by_gpa_distance_2 = Window.partitionBy("major", "cohort").orderBy(F.col("gpa_distance").desc(), "id").rowsBetween(-3, Window.currentRow)
w_distinct_major_in_cohort_dis = Window.partitionBy("distinct_major_in_cohort", "distinct_cohort_in_major_is_international")

scoped_students = (
    df_students
    .filter(~F.col("is_international"))
    .select(
        "id",
        "student_name",
        "cohort",
        "major",
        "enrolled_on",
        "gpa",
        "is_international",
        F.length(F.trim(F.col("student_name"))).alias("student_name_len"),
        F.abs(F.col("gpa") - 3.2).alias("gpa_distance"),
    )
)

windowed = (
    scoped_students
    .select(
        "*",
        F.round(F.col("gpa_distance") - F.avg("gpa_distance").over(w_major_student_name_len), 2).alias("gpa_distance_vs_major_student_name_len_avg"),
        F.round(F.cume_dist().over(w_all_by_enrolled_on_ties), 3).alias("enrolled_on_cume_dist"),
        F.max("gpa_distance").over(w_all_by_student_name_len_prio).alias("gpa_distance_max_2_prior"),
        F.count(F.lit(1)).over(w_major_is_international).alias("rows_in_major_is_international"),
        F.min("gpa_distance").over(w_major_student_name_len).alias("major_student_name_len_min_gpa_distance"),
        F.size(F.collect_set("major").over(w_cohort)).alias("distinct_major_in_cohort"),
        F.size(F.collect_set("cohort").over(w_major_is_international)).alias("distinct_cohort_in_major_is_international"),
        F.count(F.lit(1)).over(w_cohort).alias("rows_in_cohort"),
    )
)

sequenced = (
    windowed
    .select(
        "*",
        F.row_number().over(w_major_cohort_by_gpa_distance).alias("seq_no"),
        F.rank().over(w_distinct_major_in_cohort_by).alias("gpa_rank"),
        F.ntile(3).over(w_distinct_major_in_cohort_by_2).alias("gpa_tercile"),
        F.lag("enrolled_on_cume_dist", 2).over(w_major_cohort_by_gpa_distance).alias("prev2_enrolled_on_cume_dist"),
        F.lead("gpa", 2).over(w_distinct_major_in_cohort_by_2).alias("next2_gpa"),
        F.count(F.lit(1)).over(w_student_name_len_by_gpa_dist).alias("running_count"),
        F.round(F.avg("gpa").over(w_major_cohort_by_gpa_distance_2), 2).alias("gpa_avg_4"),
        F.min("gpa").over(w_distinct_major_in_cohort_dis).alias("distinct_major_in_cohort_distinct_cohort_in_majo"),
    )
)

result = (
    sequenced
    .select(
        "id",
        "student_name",
        "major",
        "enrolled_on",
        "gpa",
        "is_international",
        "student_name_len",
        "gpa_distance_vs_major_student_name_len_avg",
        "enrolled_on_cume_dist",
        "rows_in_major_is_international",
        "major_student_name_len_min_gpa_distance",
        "distinct_major_in_cohort",
        "distinct_cohort_in_major_is_international",
        "rows_in_cohort",
        "seq_no",
        "gpa_rank",
        "prev2_enrolled_on_cume_dist",
        "next2_gpa",
        "running_count",
        "gpa_avg_4",
        "distinct_major_in_cohort_distinct_cohort_in_majo",
    )
    .orderBy(
        F.col("next2_gpa").desc(),
        F.col("rows_in_cohort").desc_nulls_first(),
        "id",
    )
    .limit(20)
)
