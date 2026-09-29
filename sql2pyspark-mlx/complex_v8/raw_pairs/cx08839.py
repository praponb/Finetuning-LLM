from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_level_dept_by_credits = Window.partitionBy("level", "dept").orderBy(F.col("credits").desc(), "id")
w_level_by_credits = Window.partitionBy("level").orderBy(F.col("credits").asc_nulls_last(), "id")
w_level_by_credits_running = Window.partitionBy("level").orderBy(F.col("credits").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_dept_level = Window.partitionBy("dept", "level")

cleaned_courses = (
    df_courses
    .filter((~F.col("level").isin("intro", "advanced")) & (F.col("dept").isin("Math", "CS", "Art")))
    .select(
        "id",
        "dept",
        "credits",
        "level",
    )
)

annotated = (
    cleaned_courses
    .select(
        "id",
        "dept",
        "credits",
        "level",
        F.lower(F.trim(F.col("dept"))).alias("dept_clean"),
    )
)

metrics = (
    annotated
    .select(
        "id",
        "dept",
        "level",
        F.row_number().over(w_level_dept_by_credits).alias("rn"),
        F.row_number().over(w_level_by_credits).alias("rn_2"),
        F.first("credits", ignorenulls=True).over(w_level_by_credits_running).alias("first_known_credits"),
        F.count(F.lit(1)).over(w_dept_level).alias("rows_in_dept_level"),
    )
)

scored = (
    metrics
    .select(
        "id",
        "rn",
        "rn_2",
        "rows_in_dept_level",
        F.abs(F.col("rn_2") - 6).alias("rn_2_distance"),
    )
)

top_per_group = (
    scored
    .filter((F.col("rn_2") <= 2) & (F.col("rows_in_dept_level") >= 4))
    .drop("rn_2")
)

enrollment_stats = (
    df_enrollments
    .groupBy("course_id")
    .agg(F.max("score").alias("max_enrollment_score"))
)

summarized_rows = (
    top_per_group.alias("s")
    .join(enrollment_stats.alias("e"), F.col("e.course_id") == F.col("s.id"), "left")
    .select("s.*", "e.max_enrollment_score")
)

result = (
    summarized_rows
    .select("id", "rows_in_dept_level", "max_enrollment_score")
    .orderBy(F.col("max_enrollment_score").desc(), F.col("id").desc())
)
