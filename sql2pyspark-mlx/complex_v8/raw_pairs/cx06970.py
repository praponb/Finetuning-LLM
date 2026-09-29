from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_major_enrolled_on_quarter = Window.partitionBy("major", "enrolled_on_quarter")
w_major_by_enrolled_on_quarter = Window.partitionBy("major").orderBy("enrolled_on_quarter", "id").rowsBetween(-6, Window.currentRow)
w_is_international_flag_major = Window.partitionBy("is_international_flag", "major").orderBy(F.col("enrolled_on").desc_nulls_first(), "id").rowsBetween(-2, Window.currentRow)
w_is_international_flag_major_2 = Window.partitionBy("is_international_flag", "major").orderBy(F.col("enrolled_on").desc_nulls_first(), "id")
w_enrolled_on_month_enrolled_o = Window.partitionBy(F.month("enrolled_on"), "enrolled_on_quarter").orderBy("enrolled_on", "id")
w_major_enrolled_on_quarter_mi = Window.partitionBy("major_enrolled_on_quarter_min_is_international_f", "is_international_flag")
w_is_international_flag_max_3 = Window.partitionBy("is_international_flag_max_3").orderBy(F.col("rn").asc_nulls_last(), "id")
w_rn_is_international_flag_max = Window.partitionBy("rn", "is_international_flag_max_3").orderBy("is_international_flag")

cleaned_students = (
    df_students
    .filter((F.length(F.trim(F.col("student_name"))) >= 7) & (F.col("gpa") < 3.4))
    .select(
        "id",
        "student_name",
        "cohort",
        "major",
        "enrolled_on",
        F.when(F.col("is_international"), 1).otherwise(0).alias("is_international_flag"),
        F.quarter(F.col("enrolled_on")).alias("enrolled_on_quarter"),
    )
)

metrics = (
    cleaned_students
    .select(
        "id",
        "major",
        "is_international_flag",
        "enrolled_on_quarter",
        F.min("is_international_flag").over(w_major_enrolled_on_quarter).alias("major_enrolled_on_quarter_min_is_international_f"),
        F.round(F.avg("enrolled_on_quarter").over(w_major_by_enrolled_on_quarter), 2).alias("enrolled_on_quarter_avg_7"),
        F.max("is_international_flag").over(w_is_international_flag_major).alias("is_international_flag_max_3"),
        F.row_number().over(w_is_international_flag_major_2).alias("rn"),
        F.row_number().over(w_enrolled_on_month_enrolled_o).alias("rn_2"),
        F.round(F.percent_rank().over(w_enrolled_on_month_enrolled_o), 3).alias("enrolled_on_percent_rank"),
    )
)

windowed = (
    metrics
    .select(
        "*",
        F.round(F.col("enrolled_on_quarter_avg_7") / F.nullif(F.sum("enrolled_on_quarter_avg_7").over(w_major_enrolled_on_quarter_mi), F.lit(0)), 4).alias("enrolled_on_quarter_avg_7_share_of_major_enrolle"),
        F.round(F.avg("enrolled_on_quarter_avg_7").over(w_major_enrolled_on_quarter_mi), 2).alias("major_enrolled_on_quarter_min_is_international_f_2"),
        F.row_number().over(w_is_international_flag_max_3).alias("seq_no"),
        F.dense_rank().over(w_rn_is_international_flag_max).alias("is_international_flag_dense_rank"),
        F.count(F.lit(1)).over(w_major_enrolled_on_quarter_mi).alias("rows_in_major_enrolled_on_quarter_min_is_interna"),
    )
)

result = (
    windowed
    .select(
        "id",
        "is_international_flag",
        "enrolled_on_quarter",
        "major_enrolled_on_quarter_min_is_international_f",
        "is_international_flag_max_3",
        "rn",
        "rn_2",
        "enrolled_on_percent_rank",
        "enrolled_on_quarter_avg_7_share_of_major_enrolle",
        "major_enrolled_on_quarter_min_is_international_f_2",
        "seq_no",
        "is_international_flag_dense_rank",
        "rows_in_major_enrolled_on_quarter_min_is_interna",
        (F.col("rn") - F.col("rn_2")).alias("rn_minus_rn_2"),
        F.round(F.col("enrolled_on_percent_rank") * 1.07, 2).alias("enrolled_on_percent_rank_adj"),
    )
    .orderBy(
        F.col("major_enrolled_on_quarter_min_is_international_f_2").desc(),
        F.col("rn_minus_rn_2").desc_nulls_first(),
        "id",
    )
    .limit(5)
)
