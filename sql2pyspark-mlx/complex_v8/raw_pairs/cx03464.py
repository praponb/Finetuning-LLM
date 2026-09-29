from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_cnt_major_group = Window.partitionBy("cnt", "major_group")
w_enrollment_id_cnt_by_distinc = Window.partitionBy("enrollment_id", "cnt").orderBy("distinct_major_groups", "enrollment_id", "major_group").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_enrollment_id_cnt_by_distinc_2 = Window.partitionBy("enrollment_id", "cnt").orderBy("distinct_major_groups", "enrollment_id", "major_group")

base = (
    df_students
    .select(
        "id",
        "student_name",
        "cohort",
        "major",
        "gpa",
        "is_international",
    )
)

filtered = (
    base.alias("s")
    .join(df_enrollments.alias("e"), (F.col("e.student_id") == F.col("s.id")) & (F.col("e.grade_status").isNotNull()) & (F.col("e.score") <= 78.8), "left_anti")
)

tagged = (
    filtered
    .withColumn("major_group", F.when(F.col("major").isin("CS", "Bio"), "group_a").when(F.col("major") == "Art", "group_b").otherwise("other"))
)

exploded_detail_enrollments = (
    tagged.alias("s")
    .join(df_enrollments.alias("e"), F.col("e.student_id") == F.col("s.id"), "inner")
    .select(
        "s.id",
        "s.student_name",
        "s.major",
        "s.gpa",
        "s.is_international",
        "s.major_group",
        F.col("e.id").alias("enrollment_id"),
        F.col("e.grade_status").alias("enrollment_grade_status"),
        F.col("e.score").alias("enrollment_score"),
    )
)

grouped = (
    exploded_detail_enrollments
    .groupBy("enrollment_id", "major_group")
    .agg(
        F.array_sort(F.collect_set("student_name")).alias("student_name_list"),
        F.max("enrollment_score").alias("max_enrollment_score"),
        F.countDistinct("major_group").alias("distinct_major_groups"),
        F.count("*").alias("cnt"),
        F.round(F.sum("gpa") / F.nullif(F.sum("enrollment_score"), F.lit(0)), 3).alias("gpa_to_enrollment_score_ratio"),
        F.max("gpa").alias("max_gpa_check"),
    )
    .filter(F.col("max_gpa_check") > 2.5)
    .drop("max_gpa_check")
)

with_trends = (
    grouped
    .select(
        "enrollment_id",
        "major_group",
        "distinct_major_groups",
        "cnt",
        "gpa_to_enrollment_score_ratio",
        F.round(F.col("distinct_major_groups") - F.avg("distinct_major_groups").over(w_cnt_major_group), 2).alias("distinct_major_groups_vs_cnt_major_group_avg"),
        F.nth_value("max_enrollment_score", 2).over(w_enrollment_id_cnt_by_distinc).alias("second_max_enrollment_score"),
        F.rank().over(w_enrollment_id_cnt_by_distinc_2).alias("distinct_major_groups_rank"),
        F.max("cnt").over(w_cnt_major_group).alias("cnt_major_group_max_cnt"),
        F.round(F.col("cnt") / F.nullif(F.sum("cnt").over(w_cnt_major_group), F.lit(0)), 4).alias("cnt_share_of_cnt_major_group"),
    )
)

result = (
    with_trends
    .select(
        "enrollment_id",
        "major_group",
        "distinct_major_groups",
        "cnt",
        "gpa_to_enrollment_score_ratio",
        "distinct_major_groups_vs_cnt_major_group_avg",
        "second_max_enrollment_score",
        "distinct_major_groups_rank",
        "cnt_major_group_max_cnt",
    )
    .orderBy(F.col("distinct_major_groups").desc())
)
