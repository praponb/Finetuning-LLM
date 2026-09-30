from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_major_enrolled_on_plus_12m_d = Window.partitionBy("major", "enrolled_on_plus_12m_dayofweek").orderBy(F.col("enrolled_on_plus_12m_dayofweek").asc_nulls_last(), "id")
w_all_by_prev_enrolled_on_plus = Window.orderBy(F.col("prev_enrolled_on_plus_12m").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_prev_enrolled_on_plus_2 = Window.orderBy(F.col("prev_enrolled_on_plus_12m").desc(), "id")
w_all_by_prev_enrolled_on_plus_3 = Window.orderBy(F.col("prev_enrolled_on_plus_12m").desc())

prepared = (
    df_students
    .filter((~F.col("cohort").isin("C2022", "C2024")) & (F.col("major") == "CS"))
    .select(
        "id",
        "cohort",
        "major",
        "is_international",
        F.add_months(F.col("enrolled_on"), 12).alias("enrolled_on_plus_12m"),
    )
)

derived = (
    prepared
    .select(
        "id",
        "cohort",
        "major",
        "enrolled_on_plus_12m",
        F.substring(F.trim(F.col("cohort")), 1, 4).alias("cohort_prefix"),
        F.dayofweek(F.col("enrolled_on_plus_12m")).alias("enrolled_on_plus_12m_dayofweek"),
    )
)

with_trends = (
    derived
    .select(
        "id",
        "cohort",
        "major",
        "enrolled_on_plus_12m",
        F.lag("enrolled_on_plus_12m").over(w_major_enrolled_on_plus_12m_d).alias("prev_enrolled_on_plus_12m"),
        F.lead("enrolled_on_plus_12m").over(w_major_enrolled_on_plus_12m_d).alias("next_enrolled_on_plus_12m"),
        F.rank().over(w_major_enrolled_on_plus_12m_d).alias("enrolled_on_plus_12m_dayofweek_rank"),
    )
)

windowed = (
    with_trends
    .select(
        "id",
        "cohort",
        "prev_enrolled_on_plus_12m",
        "next_enrolled_on_plus_12m",
        "enrolled_on_plus_12m_dayofweek_rank",
        F.first("enrolled_on_plus_12m").over(w_all_by_prev_enrolled_on_plus).alias("first_enrolled_on_plus_12m"),
        F.dense_rank().over(w_all_by_prev_enrolled_on_plus_2).alias("prev_enrolled_on_plus_12m_dense_rank"),
        F.ntile(4).over(w_all_by_prev_enrolled_on_plus_2).alias("prev_enrolled_on_plus_12m_quartile"),
        F.lead("next_enrolled_on_plus_12m", 3).over(w_all_by_prev_enrolled_on_plus_2).alias("next3_next_enrolled_on_plus_12m"),
        F.dense_rank().over(w_all_by_prev_enrolled_on_plus_3).alias("prev_enrolled_on_plus_12m_dense_rank_2"),
    )
)

filtered = (
    windowed.alias("s")
    .join(df_enrollments.alias("e"), (F.col("e.student_id") == F.col("s.id")) & (F.col("e.course_id").isNotNull()), "left_semi")
    .filter(F.col("enrolled_on_plus_12m_dayofweek_rank") < 8)
)

cohort_stats = (
    filtered
    .groupBy("cohort")
    .agg(F.avg("prev_enrolled_on_plus_12m_dense_rank_2").alias("avg_prev_enrolled_on_plus_12m_dense_rank_2"))
)

vs_group = (
    filtered.alias("s")
    .join(cohort_stats.alias("g"), F.col("g.cohort") == F.col("s.cohort"), "inner")
    .filter(F.col("s.prev_enrolled_on_plus_12m_dense_rank_2") > F.col("g.avg_prev_enrolled_on_plus_12m_dense_rank_2"))
    .select(
        "s.*",
        "g.avg_prev_enrolled_on_plus_12m_dense_rank_2",
        F.round(F.col("s.prev_enrolled_on_plus_12m_dense_rank_2") - F.col("g.avg_prev_enrolled_on_plus_12m_dense_rank_2"), 2).alias("prev_enrolled_on_plus_12m_dense_rank_2_minus_avg"),
    )
)

filtered_2 = (
    vs_group
    .crossJoin(vs_group.agg(F.max("avg_prev_enrolled_on_plus_12m_dense_rank_2").alias("overall_max_avg_prev_enrolled_on_plus_12m_dense_")))
    .filter(F.col("avg_prev_enrolled_on_plus_12m_dense_rank_2") < F.col("overall_max_avg_prev_enrolled_on_plus_12m_dense_"))
    .drop("overall_max_avg_prev_enrolled_on_plus_12m_dense_")
)

result = (
    filtered_2
    .select(
        "id",
        "avg_prev_enrolled_on_plus_12m_dense_rank_2",
        "prev_enrolled_on_plus_12m_dense_rank_2_minus_avg",
    )
    .unpivot(
        ids=["id"],
        values=["avg_prev_enrolled_on_plus_12m_dense_rank_2", "prev_enrolled_on_plus_12m_dense_rank_2_minus_avg"],
        variableColumnName="measure",
        valueColumnName="amount",
    )
    .filter(F.col("amount").isNotNull())
)
