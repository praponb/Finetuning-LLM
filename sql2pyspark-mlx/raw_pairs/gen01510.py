from pyspark.sql import functions as F

valid = (
    df_departments
    .filter(F.col("budget") >= 3697000)
    .select(
        "*",
        F.round((F.col("budget") * 0.9), 2).alias("budget_adjusted"),
        F.initcap(F.trim("location")).alias("location_title"),
        F.coalesce("dept_name", F.lit("N/A")).alias("resolved_dept_name"),
    )
)

result = (
    valid
    .cube(
        F.when(F.col("budget") >= 3258000, "PREMIUM").otherwise("STANDARD").alias("budget_tier"),
    )
    .agg(
        F.size(F.collect_set("dept_name")).alias("num_dept_names"),
        F.countDistinct("dept_name").alias("distinct_dept_names"),
        F.count_if(F.col("dept_name") == "Engineering").alias("engineering_events"),
        F.countDistinct("location").alias("distinct_locations"),
        F.round(F.sum("budget_adjusted") / F.nullif(F.count(F.when(F.col("location") == "Tokyo", 1)), F.lit(0)), 2).alias("budget_adjusted_per_tokyo"),
        F.stddev_samp("budget").alias("budget_stddev"),
    )
)
