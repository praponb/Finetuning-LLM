from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("job_title") == "Intern")
    .groupBy(F.date_trunc("MONTH", "hire_date").alias("hire_month"), "is_remote")
    .agg(
        F.count(F.when(F.col("job_title") == "Designer", 1)).alias("designer_count"),
        F.sort_array(F.collect_set("job_title")).alias("job_title_list"),
    )
    .filter(F.col("designer_count") < 3)
)
