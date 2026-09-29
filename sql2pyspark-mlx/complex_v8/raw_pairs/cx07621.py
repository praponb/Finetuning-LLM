from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dept_or_unknown_dept = Window.partitionBy("dept_or_unknown", "dept")
w_all_by_credits = Window.orderBy("credits", "id")
w_all_by_credits_running = Window.orderBy("credits", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_dept_by_credits = Window.partitionBy("dept").orderBy(F.col("credits").desc(), "id")
w_last_known_credits_by_last_k = Window.partitionBy("last_known_credits").orderBy(F.col("last_known_credits").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_next_credits_credits = Window.partitionBy("next_credits", "credits")
w_credits_credits_change_by_de = Window.partitionBy("credits", "credits_change").orderBy("dept_or_unknown_dept_sum_credits", "id")

base_courses = (
    df_courses
    .filter(F.col("dept") == "Biz")
    .select(
        "id",
        "course_code",
        "dept",
        "credits",
        F.upper(F.trim(F.col("level"))).alias("level_clean"),
    )
)

scored = (
    base_courses
    .filter(F.length(F.trim(F.col("course_code"))) >= 8)
    .withColumn("dept_or_unknown", F.coalesce(F.col("dept"), F.lit("UNKNOWN")))
)

windowed = (
    scored
    .select(
        "*",
        F.sum("credits").over(w_dept_or_unknown_dept).alias("dept_or_unknown_dept_sum_credits"),
        F.lead("credits").over(w_all_by_credits).alias("next_credits"),
        (F.col("credits") - F.lag("credits").over(w_all_by_credits)).alias("credits_change"),
        F.last("credits", ignorenulls=True).over(w_all_by_credits_running).alias("last_known_credits"),
        (F.col("credits") - F.lag("credits").over(w_dept_by_credits)).alias("credits_change_2"),
    )
)

screened = (
    windowed.alias("s")
    .join(df_enrollments.alias("e"), (F.col("e.course_id") == F.col("s.id")) & (F.col("e.grade_status") == "PASS"), "left_semi")
    .filter(F.col("level_clean").isNotNull())
)

dept_or_unknown_stats = (
    screened
    .groupBy("dept_or_unknown")
    .agg(F.avg("credits_change").alias("avg_credits_change"))
)

compared = (
    screened.alias("s")
    .join(dept_or_unknown_stats.alias("g"), F.col("g.dept_or_unknown") == F.col("s.dept_or_unknown"), "inner")
    .filter(F.col("s.credits_change") > F.col("g.avg_credits_change"))
    .select(
        "s.*",
        "g.avg_credits_change",
        F.round(F.col("s.credits_change") - F.col("g.avg_credits_change"), 2).alias("credits_change_minus_avg"),
    )
)

labeled = (
    compared
    .filter(F.col("last_known_credits") > 2)
    .select(
        "id",
        "course_code",
        "credits",
        "dept_or_unknown",
        "dept_or_unknown_dept_sum_credits",
        "next_credits",
        "credits_change",
        "last_known_credits",
        "credits_change_minus_avg",
        F.lower(F.trim(F.col("dept_or_unknown"))).alias("dept_or_unknown_clean"),
    )
)

windowed_2 = (
    labeled
    .select(
        "id",
        "course_code",
        "credits",
        "dept_or_unknown",
        "dept_or_unknown_dept_sum_credits",
        "next_credits",
        "credits_change",
        "last_known_credits",
        "credits_change_minus_avg",
        F.first("credits_change_minus_avg").over(w_last_known_credits_by_last_k).alias("first_credits_change_minus_avg"),
        F.round(F.col("next_credits") - F.avg("next_credits").over(w_next_credits_credits), 2).alias("next_credits_vs_next_credits_credits_avg"),
        F.rank().over(w_credits_credits_change_by_de).alias("dept_or_unknown_dept_sum_credits_rank"),
    )
)

result = (
    windowed_2
    .select(
        "id",
        "course_code",
        "dept_or_unknown_dept_sum_credits",
        "next_credits",
        "credits_change",
        "last_known_credits",
        "credits_change_minus_avg",
        "first_credits_change_minus_avg",
        "next_credits_vs_next_credits_credits_avg",
        F.when(F.col("dept_or_unknown").isin("CS", "Art"), "group_a").when(F.col("dept_or_unknown") == "Math", "group_b").otherwise("other").alias("dept_or_unknown_group"),
        F.round(F.col("credits_change_minus_avg") * 1.1, 2).alias("credits_change_minus_avg_adj"),
    )
    .orderBy(F.col("next_credits").desc())
)
