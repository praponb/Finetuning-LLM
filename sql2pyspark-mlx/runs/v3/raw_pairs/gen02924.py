from pyspark.sql import functions as F

result = (
    df_employees
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("job_title"))).alias("job_title_values"),
        F.max("hire_date").alias("last_hire_date"),
    )
    .orderBy("last_hire_date")
)
