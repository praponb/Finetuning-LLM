from pyspark.sql import functions as F

result = (
    df_employees
    .filter((F.col("dept_id") == 1) & ~F.col("email").like("%@startup.io") & F.col("full_name").isNull())
    .groupBy("job_title")
    .agg(
        F.round(F.avg("salary"), 2).alias("avg_salary"),
        F.sort_array(F.collect_set("job_title")).alias("job_title_list"),
        F.sum(F.when(F.col("level") == "L4", 1).otherwise(0)).alias("l4_count"),
    )
    .filter(F.col("l4_count") < 66)
    .drop("l4_count")
    .orderBy("job_title")
)
