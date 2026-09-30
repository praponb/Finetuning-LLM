from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_credits_credits_distance = Window.partitionBy("credits", "credits_distance")
w_level_credits_by_credits_dis = Window.partitionBy("level", "credits").orderBy("credits_distance", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_dept_credits_distance = Window.partitionBy("dept", "credits_distance")
w_all_by_first_credits_ties = Window.orderBy(F.col("first_credits").asc_nulls_last())

valid = (
    df_courses
    .filter(F.col("dept") == "CS")
    .select(
        "id",
        "course_code",
        "dept",
        "credits",
        "level",
        F.upper(F.trim(F.col("dept"))).alias("dept_clean"),
        F.abs(F.col("credits") - 2).alias("credits_distance"),
    )
)

metrics = (
    valid
    .withColumn("rows_in_credits_credits_distance", F.count(F.lit(1)).over(w_credits_credits_distance))
    .withColumn("first_credits", F.first("credits").over(w_level_credits_by_credits_dis))
)

sequenced = (
    metrics
    .select(
        "id",
        "course_code",
        "dept",
        "level",
        "dept_clean",
        "credits_distance",
        "rows_in_credits_credits_distance",
        "first_credits",
        F.count(F.lit(1)).over(w_dept_credits_distance).alias("rows_in_dept_credits_distance"),
        F.round(F.cume_dist().over(w_all_by_first_credits_ties), 3).alias("first_credits_cume_dist"),
        F.max("credits_distance").over(w_dept_credits_distance).alias("dept_credits_distance_max_credits_distance"),
    )
)

result = (
    sequenced
    .select(
        "id",
        "course_code",
        "level",
        "dept_clean",
        "credits_distance",
        "rows_in_credits_credits_distance",
        "first_credits",
        "first_credits_cume_dist",
        "dept_credits_distance_max_credits_distance",
    )
)
