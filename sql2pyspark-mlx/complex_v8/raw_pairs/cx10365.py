from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_term_by_first_enrolled_at_da = Window.partitionBy("term").orderBy("first_enrolled_at_date", "term", "course_id")
w_distinct_term_groups_course = Window.partitionBy("distinct_term_groups", "course_id")
w_distinct_term_groups_by_next = Window.partitionBy("distinct_term_groups").orderBy("next_last_enrolled_at", "term", "course_id")
w_last_enrolled_at_month_by_av = Window.partitionBy(F.month("last_enrolled_at")).orderBy("avg_score", "term", "course_id")

filtered_enrollments = (
    df_enrollments
    .filter(F.hour(F.col("enrolled_at")).between(10, 16))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "enrolled_at",
        F.to_date(F.col("enrolled_at")).alias("enrolled_at_date"),
        F.when(F.col("term").isin("2025T2", "2025T1"), "group_a").when(F.col("term") == "2024T1", "group_b").otherwise("other").alias("term_group"),
    )
)

grouped = (
    filtered_enrollments
    .groupBy("term", "course_id")
    .agg(
        F.min("enrolled_at_date").alias("first_enrolled_at_date"),
        F.round(F.avg("score"), 2).alias("avg_score"),
        F.count("*").alias("cnt"),
        F.countDistinct("term_group").alias("distinct_term_groups"),
        F.array_sort(F.collect_set("term")).alias("term_list"),
        F.max("enrolled_at").alias("last_enrolled_at"),
    )
    .filter(F.col("cnt") >= 3)
)

windowed = (
    grouped
    .select(
        "term",
        "course_id",
        "first_enrolled_at_date",
        "avg_score",
        "distinct_term_groups",
        "last_enrolled_at",
        F.lead("last_enrolled_at").over(w_term_by_first_enrolled_at_da).alias("next_last_enrolled_at"),
        F.count(F.lit(1)).over(w_distinct_term_groups_course).alias("rows_in_distinct_term_groups_course_id"),
    )
)

with_windows = (
    windowed
    .select(
        "term",
        "course_id",
        "first_enrolled_at_date",
        "avg_score",
        "distinct_term_groups",
        "last_enrolled_at",
        "next_last_enrolled_at",
        "rows_in_distinct_term_groups_course_id",
        F.dense_rank().over(w_distinct_term_groups_by_next).alias("next_last_enrolled_at_dense_rank"),
        F.ntile(4).over(w_last_enrolled_at_month_by_av).alias("avg_score_quartile"),
    )
)

screened = (
    with_windows.alias("s")
    .join(df_courses.alias("c"), (F.col("c.id") == F.col("s.course_id")) & (F.col("c.dept") == "Biz"), "left_semi")
)

qualified = (
    screened.alias("s")
    .join(df_enrollments.alias("e"), (F.col("e.course_id") == F.col("s.course_id")) & (F.month(F.col("e.enrolled_at")).isin(1, 2)), "left_semi")
    .filter(F.col("avg_score_quartile").between(3, 8))
)

result = (
    qualified
    .select(
        "term",
        "course_id",
        "next_last_enrolled_at_dense_rank",
        "rows_in_distinct_term_groups_course_id",
    )
    .unpivot(
        ids=["term", "course_id"],
        values=["next_last_enrolled_at_dense_rank", "rows_in_distinct_term_groups_course_id"],
        variableColumnName="measure",
        valueColumnName="amount",
    )
    .filter(F.col("amount").isNotNull())
)
