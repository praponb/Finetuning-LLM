from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("manager_id").isin([5, 23, 31]))
    .groupBy("job_title")
    .agg(
        F.sum(F.when(F.col("job_title") == "Designer", F.col("salary")).otherwise(0)).alias("designer_salary"),
        F.sum(F.when(F.col("job_title") == "Analyst", F.col("salary")).otherwise(0)).alias("analyst_salary"),
    )
    .filter(F.col("designer_salary") >= 66)
    .select("job_title", "designer_salary", "analyst_salary")
    .orderBy(F.col("analyst_salary").desc())
)
