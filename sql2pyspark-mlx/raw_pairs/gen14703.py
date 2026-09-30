from pyspark.sql import functions as F

valid = (
    df_departments
    .filter(F.col("location").isNotNull() & (~F.col("location").eqNullSafe(F.lit("New York"))))
    .select(
        "*",
        F.concat_ws(",", "dept_name", "location").alias("dept_name_location_key"),
        F.coalesce("dept_name", F.lit("NONE")).alias("dept_name_clean"),
    )
)

result = (
    valid
    .groupBy(
        F.when(F.col("budget") >= 3823000, "HIGH").otherwise("LOW").alias("budget_tier"),
    )
    .agg(
        F.count_if(F.col("location") == "Tokyo").alias("tokyo_events"),
        F.size(F.collect_set("dept_name")).alias("num_dept_names"),
        F.countDistinct("dept_name").alias("distinct_dept_names"),
    )
    .filter(F.col("distinct_dept_names") > 1)
)
