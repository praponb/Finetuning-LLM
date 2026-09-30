from pyspark.sql import functions as F

result = (
    df_employees
    .cube("level", "is_remote")
    .agg(
        F.round(F.avg("salary"), 2).alias("avg_salary"),
        F.concat_ws(", ", F.sort_array(F.collect_set("job_title"))).alias("job_title_values"),
    )
)
