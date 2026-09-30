from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_enrolled_on_month = Window.partitionBy(F.date_trunc("month", "enrolled_on"))
w_major_by_gpa = Window.partitionBy("major").orderBy(F.col("gpa").desc(), "id")
w_major_by_enrolled_on = Window.partitionBy("major").orderBy(F.col("enrolled_on").desc_nulls_first(), "id")
w_all_by_gpa_share_of_enrolled = Window.orderBy(F.col("gpa_share_of_enrolled_on_month").desc(), "id")
w_all_by_gpa_share_of_enrolled_2 = Window.orderBy(F.col("gpa_share_of_enrolled_on_month").desc(), "id").rowsBetween(-3, Window.currentRow)

base_students = (
    df_students
    .filter(F.col("enrolled_on").between(F.lit("2024-04-04").cast("date"), F.lit("2025-08-27").cast("date")))
    .select(
        "id",
        "major",
        "enrolled_on",
        "gpa",
    )
)

with_trends = (
    base_students
    .select(
        "id",
        "enrolled_on",
        F.round(F.col("gpa") / F.nullif(F.sum("gpa").over(w_enrolled_on_month), F.lit(0)), 4).alias("gpa_share_of_enrolled_on_month"),
        F.datediff(F.col("enrolled_on"), F.lag("enrolled_on").over(w_major_by_gpa)).alias("days_since_prev_enrolled_on"),
        F.round(F.col("gpa") - F.avg("gpa").over(w_enrolled_on_month), 2).alias("gpa_vs_enrolled_on_month_avg"),
        (F.col("gpa") - F.lag("gpa").over(w_major_by_gpa)).alias("gpa_change"),
        (F.col("gpa") - F.lag("gpa").over(w_major_by_enrolled_on)).alias("gpa_change_2"),
        F.round(F.avg("gpa").over(w_enrolled_on_month), 2).alias("enrolled_on_month_avg_gpa"),
    )
)

windowed = (
    with_trends
    .select(
        "id",
        "days_since_prev_enrolled_on",
        "gpa_change_2",
        "enrolled_on_month_avg_gpa",
        F.round(F.percent_rank().over(w_all_by_gpa_share_of_enrolled), 3).alias("gpa_share_of_enrolled_on_month_percent_rank"),
        F.sum("gpa_change_2").over(w_all_by_gpa_share_of_enrolled_2).alias("gpa_change_2_sum_4"),
    )
)

result = (
    windowed
    .select(
        "id",
        F.col("enrolled_on_month_avg_gpa").alias("enrolled_on_month_avg_gpa_v"),
        F.col("gpa_change_2").alias("m_gpa_change_2"),
    )
    .unpivot(
        ids=["id"],
        values=["enrolled_on_month_avg_gpa_v", "m_gpa_change_2"],
        variableColumnName="measure",
        valueColumnName="value",
    )
    .filter(F.col("value").isNotNull())
    .orderBy("id", "measure")
)
