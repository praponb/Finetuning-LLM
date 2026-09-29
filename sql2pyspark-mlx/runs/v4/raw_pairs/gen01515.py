from pyspark.sql import functions as F

cleaned = (
    df_departments
    .filter(F.col("dept_name").isNotNull() & ~F.col("location").isin(["Tokyo", "Bangkok", "Singapore"]))
    .withColumn("dept_name_location_key", F.concat_ws("-", "dept_name", "location"))
    .withColumn("dept_name_title", F.initcap(F.trim("dept_name")))
    .withColumn("resolved_dept_name", F.coalesce("dept_name", F.lit("OTHER")))
)

base = (
    cleaned
    .filter(~F.col("dept_name").isin(["Legal", "Sales"]))
    .select(
        "id",
        "dept_name",
        "location",
        "dept_name_title",
        F.when(F.col("budget") >= 4358000, "High").when(F.col("budget") >= 940000, "Medium").otherwise("Low").alias("budget_tier"),
    )
)

not_flagged = (
    base
    .alias("s")
    .join(df_employees.alias("ch"), (F.col("ch.dept_id") == F.col("s.id")) & (F.col("ch.manager_id").isin([13, 19, 39])), "left_semi")
)

not_flagged_departments = (
    not_flagged
    .alias("s")
    .join(df_employees.alias("x"), (F.col("x.dept_id") == F.col("s.id")) & (F.col("x.dept_id").isin([7, 14, 17, 29])), "left_semi")
)

filtered = (
    not_flagged_departments
    .filter(F.col("location").isin(["Bangkok", "London"]))
    .withColumn("dept_name_lower", F.lower("dept_name"))
)

stats = (
    filtered
    .groupBy("location")
    .agg(
        F.size(F.collect_set("location")).alias("num_locations"),
        F.count("*").alias("row_count"),
        F.count(F.when(F.col("location") == "London", 1)).alias("london_count"),
        F.count("*").alias("row_count_2"),
    )
    .filter(F.col("london_count") < 1)
)

result = (
    stats
    .select(
        "location",
        "row_count",
        "london_count",
        F.when(F.col("location").rlike("^(London|Singapore)$"), "OTHER_ZONE").otherwise("GROUP_A").alias("location_zone"),
    )
    .orderBy(F.col("row_count").asc_nulls_last(), "london_count")
)
