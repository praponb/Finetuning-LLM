from pyspark.sql import functions as F

recent_employees = (
    df_employees
    .filter(F.col("manager_id").isNotNull() & F.col("email").like("%@gmail.com") & (F.lower(F.trim("email")) == "bob.smith@yahoo.com"))
)

employees_stats = (
    recent_employees
    .groupBy("level")
    .agg(
        F.sum(F.when(F.col("job_title") == "Manager", F.col("salary")).otherwise(0)).alias("manager_salary"),
        F.max("salary").alias("max_salary"),
    )
)

result = (
    employees_stats
    .filter(F.col("max_salary") > 148000)
    .select("level", "manager_salary", "max_salary")
    .orderBy(F.col("manager_salary").desc(), "max_salary", "level")
    .limit(3)
)
