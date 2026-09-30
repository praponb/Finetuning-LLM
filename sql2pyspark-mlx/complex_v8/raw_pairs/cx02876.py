from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_gpa = Window.orderBy("gpa", "id")
w_is_international = Window.partitionBy("is_international")
w_gpa_dense_rank_by_enrolled_o = Window.partitionBy("gpa_dense_rank").orderBy("enrolled_on", "id")
w_gpa_dense_rank_by_enrolled_o_2 = Window.partitionBy("gpa_dense_rank").orderBy("enrolled_on")
w_gpa_quintile_gpa_dense_rank = Window.partitionBy("gpa_quintile", "gpa_dense_rank").orderBy(F.col("gpa_filled").desc(), "id")
w_gpa_dense_rank_by_enrolled_o_3 = Window.partitionBy("gpa_dense_rank").orderBy("enrolled_on", "id").rowsBetween(-1, Window.currentRow)
w_gpa_dense_rank = Window.partitionBy("gpa_dense_rank")

filtered = (
    df_students
    .filter((F.col("major").isNotNull()) | (F.col("is_international")))
    .select(
        "id",
        "student_name",
        "major",
        "enrolled_on",
        "gpa",
        "is_international",
        F.coalesce(F.col("gpa"), F.lit(0.0)).alias("gpa_filled"),
        F.month(F.col("enrolled_on")).alias("enrolled_on_month"),
        F.concat_ws(" / ", F.col("major"), F.col("student_name")).alias("major_student_name_label"),
    )
)

sequenced = (
    filtered
    .select(
        "id",
        "enrolled_on",
        "gpa",
        "gpa_filled",
        F.round(F.percent_rank().over(w_all_by_gpa), 3).alias("gpa_percent_rank"),
        F.round(F.col("gpa_filled") - F.avg("gpa_filled").over(w_is_international), 2).alias("gpa_filled_vs_is_international_avg"),
        F.round(F.col("gpa") / F.nullif(F.sum("gpa").over(w_is_international), F.lit(0)), 4).alias("gpa_share_of_is_international"),
        F.ntile(5).over(w_all_by_gpa).alias("gpa_quintile"),
        F.round(F.col("enrolled_on_month") / F.nullif(F.sum("enrolled_on_month").over(w_is_international), F.lit(0)), 4).alias("enrolled_on_month_share_of_is_international"),
        F.dense_rank().over(w_all_by_gpa).alias("gpa_dense_rank"),
    )
)

sequenced_2 = (
    sequenced
    .select(
        "id",
        "enrolled_on",
        "gpa",
        "gpa_filled",
        "gpa_percent_rank",
        "gpa_filled_vs_is_international_avg",
        "gpa_quintile",
        "enrolled_on_month_share_of_is_international",
        "gpa_dense_rank",
        F.row_number().over(w_gpa_dense_rank_by_enrolled_o).alias("enrolled_on_seq"),
        F.rank().over(w_gpa_dense_rank_by_enrolled_o_2).alias("enrolled_on_rank"),
        F.round(F.cume_dist().over(w_gpa_quintile_gpa_dense_rank), 3).alias("gpa_filled_cume_dist"),
        F.round(F.avg("gpa").over(w_gpa_dense_rank_by_enrolled_o_3), 2).alias("gpa_avg_2"),
        F.count(F.lit(1)).over(w_gpa_dense_rank).alias("rows_in_gpa_dense_rank"),
    )
)

screened = (
    sequenced_2.alias("s")
    .join(df_enrollments.alias("e"), (F.col("e.student_id") == F.col("s.id")) & (F.col("e.score") < 71.0) & (F.col("e.grade_status") == "INCOMPLETE"), "left_anti")
)

labeled = (
    screened
    .select(
        "*",
        F.round(F.col("gpa_filled") * 1.5, 2).alias("gpa_filled_adj"),
        F.dayofweek(F.col("enrolled_on")).alias("enrolled_on_dayofweek"),
    )
)

result = (
    labeled
    .select("enrolled_on_rank", "gpa_dense_rank")
    .distinct()
    .orderBy(F.col("enrolled_on_rank").desc())
)
