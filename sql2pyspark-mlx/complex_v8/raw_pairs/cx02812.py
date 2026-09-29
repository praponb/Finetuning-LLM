from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_enrolled_on_ties = Window.orderBy(F.col("enrolled_on").desc())
w_all_by_enrolled_on_running = Window.orderBy(F.col("enrolled_on").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_enrolled_on = Window.orderBy(F.col("enrolled_on").desc(), "id")
w_major_by_gpa = Window.partitionBy("major").orderBy(F.col("gpa").desc(), "id")
w_major = Window.partitionBy("major")
w_all_by_enrolled_on_moving_6 = Window.orderBy(F.col("enrolled_on").desc(), "id").rowsBetween(-6, Window.currentRow)

valid_students = (
    df_students
    .filter(F.col("enrolled_on").isNotNull())
    .select(
        "id",
        "student_name",
        "major",
        "enrolled_on",
        "gpa",
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("enrolled_on")).alias("days_since_enrolled_on"),
        F.lower(F.regexp_replace(F.trim(F.col("student_name")), " ", "_")).alias("student_name_slug"),
        F.round(F.col("gpa") * 0.9, 2).alias("gpa_adj"),
    )
)

screened = (
    valid_students
    .crossJoin(valid_students.agg(F.avg("gpa").alias("overall_avg_gpa")))
    .filter(F.col("gpa") > F.col("overall_avg_gpa"))
    .drop("overall_avg_gpa")
)

windowed = (
    screened
    .select(
        "*",
        F.dense_rank().over(w_all_by_enrolled_on_ties).alias("enrolled_on_dense_rank"),
        F.count(F.lit(1)).over(w_all_by_enrolled_on_running).alias("running_count"),
        F.ntile(3).over(w_all_by_enrolled_on).alias("enrolled_on_tercile"),
        F.lead("gpa_adj").over(w_major_by_gpa).alias("next_gpa_adj"),
        F.last("gpa_adj", ignorenulls=True).over(w_all_by_enrolled_on_running).alias("last_known_gpa_adj"),
        F.round(F.avg("gpa_adj").over(w_all_by_enrolled_on_moving_6), 2).alias("gpa_adj_avg_7"),
    )
)

result = (
    windowed
    .groupBy("enrolled_on_tercile")
    .pivot("major", ["Art", "CS", "Biz", "Bio"])
    .agg(F.sum("days_since_enrolled_on").alias("total"), F.count("days_since_enrolled_on").alias("n"))
)
