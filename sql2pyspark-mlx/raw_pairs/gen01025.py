from pyspark.sql import functions as F

base = (
    df_employees
    .filter((F.col("salary") < 105000) & (F.col("full_name").like("%Chaiyap%") | ~F.col("level").isin(["L4", "L3"])))
    .withColumn("full_name_lower", F.lower("full_name"))
)

employees_stats = (
    base
    .groupBy("is_remote", "job_title")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("job_title"))).alias("job_title_values"),
        F.round(F.stddev("salary"), 2).alias("stddev_salary"),
    )
)

result = (
    employees_stats
    .select("is_remote", "job_title", "job_title_values", "stddev_salary")
    .orderBy(
        F.col("stddev_salary").desc(),
        "job_title_values",
        "is_remote",
        "job_title",
    )
    .limit(5)
)
