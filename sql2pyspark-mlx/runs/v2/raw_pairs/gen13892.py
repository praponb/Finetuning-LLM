from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("hire_date") <= "2025-06-01")
    .groupBy(
        F.year("hire_date").alias("hire_year"),
        F.date_trunc("MONTH", "hire_date").alias("hire_month"),
    )
    .agg(
        F.count("*").alias("total_count"),
        F.sort_array(F.collect_set("job_title")).alias("job_title_list"),
    )
    .filter(F.col("total_count") > 7)
    .orderBy(F.col("total_count").desc(), "hire_year", "hire_month")
    .limit(5)
)
