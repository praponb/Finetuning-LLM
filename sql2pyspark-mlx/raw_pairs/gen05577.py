from pyspark.sql import functions as F

src = (
    df_employees
    .filter(F.col("dept_id").isin([4, 17, 29, 36]) & F.col("email").isNotNull())
    .withColumn("salary_whole", F.floor("salary"))
)

employees_stats = (
    src
    .groupBy(F.year("hire_date").alias("hire_year"))
    .agg(
        F.min("salary_whole").alias("min_salary_whole"),
        F.max("salary_whole").alias("max_salary_whole"),
    )
)

result = (
    employees_stats
    .filter(F.col("min_salary_whole") > 40)
    .select("hire_year", "min_salary_whole")
    .orderBy(F.col("hire_year").desc())
)
