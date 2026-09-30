from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("hire_date").between("2024-01-07", "2025-08-01"))
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("job_title"))).alias("job_title_values"),
    )
)
