from pyspark.sql import functions as F

src = df_employees.filter(~(F.col("is_remote") | F.col("dept_id").isin([13, 39, 40])))

summary = (
    src
    .groupBy("job_title", F.year("hire_date").alias("hire_year"))
    .agg(F.sort_array(F.collect_set("job_title")).alias("job_title_list"))
)

result = (
    summary
    .select("job_title", "job_title_list")
    .orderBy("job_title")
)
