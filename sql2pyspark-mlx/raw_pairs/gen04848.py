from pyspark.sql import functions as F

result = (
    df_employees
    .groupBy(F.quarter("hire_date").alias("hire_quarter"))
    .agg(
        F.count(F.when(F.col("job_title") == "Analyst", 1)).alias("analyst_count"),
        F.sort_array(F.collect_set("job_title")).alias("job_title_list"),
    )
    .orderBy(F.col("analyst_count").desc(), F.col("hire_quarter").desc())
)
