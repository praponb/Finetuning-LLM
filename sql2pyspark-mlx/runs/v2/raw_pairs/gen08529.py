from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("salary").between(74000, 106000) & (F.col("hire_date") >= "2023-05-01"))
    .groupBy("job_title")
    .agg(
        (F.max("salary") - F.min("salary")).alias("salary_range"),
        F.max("hire_date").alias("latest_hire_date"),
        F.concat_ws(", ", F.sort_array(F.collect_set("job_title"))).alias("job_title_values"),
    )
    .orderBy("latest_hire_date", "job_title")
    .limit(10)
)
