from pyspark.sql import functions as F

result = (
    df_employees
    .groupBy("manager_id")
    .agg(
        F.count(F.when(F.col("job_title") == "Analyst", 1)).alias("analyst_count"),
        (F.max("salary") - F.min("salary")).alias("salary_range"),
        F.countDistinct("salary").alias("unique_managers"),
    )
    .orderBy("unique_managers", "manager_id")
    .limit(5)
)
