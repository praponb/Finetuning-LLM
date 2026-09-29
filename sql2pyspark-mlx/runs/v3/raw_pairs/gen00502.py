from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("level").isNull() & F.col("dept_id").isin([13, 28]))
    .groupBy("is_remote")
    .agg(
        F.max("salary").alias("max_salary"),
        (F.max("salary") - F.min("salary")).alias("salary_range"),
    )
    .orderBy("salary_range", "max_salary")
)
