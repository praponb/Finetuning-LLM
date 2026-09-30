from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("full_name").like("%Ivy%"))
    .groupBy("manager_id", "is_remote")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("job_title"))).alias("job_title_values"),
        F.sum(F.when(F.col("job_title") == "Intern", 1).otherwise(0)).alias("intern_count"),
    )
    .filter(F.col("intern_count") < 42)
    .drop("intern_count")
    .orderBy("job_title_values")
)
