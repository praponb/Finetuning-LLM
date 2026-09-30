from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("dept_id").isin([17, 21, 34]))
    .groupBy("level")
    .agg(
        F.countDistinct("salary").alias("unique_depts"),
        F.min("salary").alias("min_salary"),
        (F.max("salary") - F.min("salary")).alias("salary_range"),
    )
    .orderBy(F.col("level").desc(), "salary_range")
)
