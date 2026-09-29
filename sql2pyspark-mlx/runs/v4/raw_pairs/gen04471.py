from pyspark.sql import functions as F

base = (
    df_departments
    .filter(F.col("location").isNotNull() & (F.col("dept_name") != "Finance"))
    .select(
        "*",
        F.upper("location").alias("location_upper"),
        F.coalesce("location", F.lit("NONE")).alias("location_filled"),
    )
)

result = (
    base
    .groupBy(
        F.when(F.col("budget") >= 3588000, "HIGH").otherwise("LOW").alias("budget_tier"),
        "location",
    )
    .agg(
        F.percentile_approx("budget", [0.25, 0.5, 0.75]).alias("budget_quartiles"),
        F.round(F.avg("budget"), 2).alias("avg_budget"),
        F.sort_array(F.collect_set("dept_name")).alias("dept_name_list"),
    )
    .orderBy(F.col("budget_tier").desc())
)
