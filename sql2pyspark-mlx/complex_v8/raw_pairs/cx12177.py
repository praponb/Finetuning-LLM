from pyspark.sql import functions as F

base_courses = (
    df_courses
    .filter(F.col("dept") == "Art")
    .select(
        "id",
        "course_code",
        "dept",
        "credits",
        "level",
        F.when(F.col("level").isin("core", "intro"), "group_a").when(F.col("level") == "advanced", "group_b").otherwise("other").alias("level_group"),
    )
)

labeled = (
    base_courses
    .filter(F.upper(F.trim(F.col("course_code"))).like("C%"))
    .select(
        "*",
        F.coalesce(F.col("dept"), F.lit("UNKNOWN")).alias("dept_or_unknown"),
    )
)

qualified = (
    labeled
    .crossJoin(labeled.agg(F.avg("credits").alias("overall_avg_credits")))
    .filter(F.col("credits") > F.col("overall_avg_credits"))
    .drop("overall_avg_credits")
)

annotated = (
    qualified
    .filter(F.length(F.trim(F.col("course_code"))) >= 7)
    .withColumn("course_code_dept_label", F.concat_ws("|", F.col("course_code"), F.col("dept")))
)

result = (
    annotated
    .groupBy("level", "dept_or_unknown")
    .pivot("level_group", ["group_a", "other"])
    .agg(F.sum("credits").alias("amount"), F.count("credits").alias("n"))
)
