from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_enrolled_on_month_is_interna = Window.partitionBy(F.date_trunc("month", "enrolled_on"), "is_international").orderBy("days_since_enrolled_on", "id").rowsBetween(-3, Window.currentRow)
w_all_by_gpa_adj_full = Window.orderBy("gpa_adj", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_dedup = Window.partitionBy("cohort").orderBy(F.col("enrolled_on").desc(), F.col("id").desc())

valid_students = (
    df_students
    .filter((~F.col("major").isin("Art", "Math")) & (F.col("gpa") > 2.0))
    .select(
        "id",
        "student_name",
        "cohort",
        "enrolled_on",
        "is_international",
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("enrolled_on")).alias("days_since_enrolled_on"),
        F.substring(F.trim(F.col("major")), 1, 3).alias("major_prefix"),
        F.round(F.col("gpa") * 0.9, 2).alias("gpa_adj"),
    )
)

metrics = (
    valid_students
    .select(
        "id",
        "student_name",
        "cohort",
        "enrolled_on",
        "is_international",
        "days_since_enrolled_on",
        "gpa_adj",
        F.round(F.avg("days_since_enrolled_on").over(w_enrolled_on_month_is_interna), 2).alias("days_since_enrolled_on_avg_4"),
        F.nth_value("days_since_enrolled_on", 2).over(w_all_by_gpa_adj_full).alias("second_days_since_enrolled_on"),
    )
)

latest_per_key = (
    metrics
    .withColumn("row_num", F.row_number().over(w_dedup))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

result = (
    latest_per_key
    .select(
        "id",
        "student_name",
        "cohort",
        "enrolled_on",
        "days_since_enrolled_on",
        "gpa_adj",
        "days_since_enrolled_on_avg_4",
        "second_days_since_enrolled_on",
        F.when(F.col("days_since_enrolled_on") >= 560, "large").when(F.col("days_since_enrolled_on") >= 210, "medium").otherwise("small").alias("days_since_enrolled_on_tier"),
    )
    .orderBy(
        F.col("enrolled_on").desc(),
        F.col("second_days_since_enrolled_on").desc(),
    )
)
