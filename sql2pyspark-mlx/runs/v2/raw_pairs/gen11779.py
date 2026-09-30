from pyspark.sql import functions as F

result = (
    df_employees
    .filter(((F.col("dept_id") == 20) & (F.col("manager_id") == 26)) | F.upper("email").like("%ANY%"))
    .groupBy(
        F.when(F.col("salary") >= 118000, "high").when(F.col("salary") >= 83000, "mid").otherwise("low").alias("salary_bucket"),
    )
    .agg(
        F.max("hire_date").alias("last_hire_date"),
        F.round(F.stddev("salary"), 2).alias("stddev_salary"),
    )
    .filter(F.col("stddev_salary") >= 54)
    .orderBy(F.col("stddev_salary").desc(), "salary_bucket")
    .limit(3)
)
