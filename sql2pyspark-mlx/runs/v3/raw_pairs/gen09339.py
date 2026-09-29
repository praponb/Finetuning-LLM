from pyspark.sql import functions as F

result = (
    df_employees
    .filter(((F.col("salary") >= 115000) & (F.col("hire_date") <= "2025-01-01")) | (F.col("dept_id") == 19))
    .groupBy("dept_id")
    .agg(
        F.sort_array(F.collect_set("job_title")).alias("job_title_list"),
        F.max("hire_date").alias("latest_hire_date"),
    )
    .orderBy(F.col("dept_id").desc(), F.col("latest_hire_date").desc())
    .limit(50)
)
