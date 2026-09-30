from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("manager_id").isin([13, 15, 28, 37]) & F.col("manager_id").isNotNull())
    .select(
        "hire_date",
        F.year("hire_date").alias("hire_date_year"),
        F.ceil("salary").alias("salary_whole"),
    )
    .orderBy("hire_date", F.col("hire_date_year").desc())
)
