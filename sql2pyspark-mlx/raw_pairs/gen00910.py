from pyspark.sql import functions as F

result = (
    df_employees
    .groupBy(F.date_trunc("MONTH", "hire_date").alias("hire_month"))
    .agg(
        F.max("hire_date").alias("last_hire_date"),
        F.concat_ws(", ", F.sort_array(F.collect_set("job_title"))).alias("job_title_values"),
        F.count("*").alias("total_count"),
    )
)
