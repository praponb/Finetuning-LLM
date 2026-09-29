from pyspark.sql import functions as F

result = (
    df_employees
    .filter((F.col("manager_id") == 38) & (F.col("salary") > 96000))
    .groupBy(F.year("hire_date").alias("hire_year"), "job_title", "level")
    .agg(
        F.sum("salary").alias("total_salary"),
        F.sort_array(F.collect_set("job_title")).alias("job_title_list"),
    )
)
