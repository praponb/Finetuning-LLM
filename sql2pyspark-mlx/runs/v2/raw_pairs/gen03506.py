from pyspark.sql import functions as F

result = (
    df_employees
    .groupBy("is_remote")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("job_title"))).alias("job_title_values"),
        F.round(F.avg("salary"), 2).alias("avg_salary"),
    )
    .filter(F.col("avg_salary") < 30)
    .select("is_remote", "job_title_values", "avg_salary")
    .orderBy("job_title_values", "is_remote")
    .limit(10)
)
