from pyspark.sql import functions as F

result = (
    df_employees
    .groupBy(F.year("hire_date").alias("hire_year"))
    .agg(
        F.sort_array(F.collect_set("job_title")).alias("job_title_list"),
        F.count("*").alias("employee_count"),
    )
    .filter(F.col("employee_count") < 2)
    .drop("employee_count")
    .orderBy(F.col("hire_year").desc())
    .limit(20)
)
