from pyspark.sql import functions as F

result = (
    df_employees
    .groupBy(
        F.when(F.col("job_title").isin(["Director", "Manager"]), "group_a").otherwise("other").alias("job_title_group"),
    )
    .agg(
        F.sum(F.when(F.col("job_title") == "Analyst", F.col("salary")).otherwise(0)).alias("analyst_salary"),
        F.max("salary").alias("max_salary"),
    )
)
