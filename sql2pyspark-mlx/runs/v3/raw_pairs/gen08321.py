from pyspark.sql import functions as F

result = (
    df_employees
    .groupBy(
        "is_remote",
        F.year("hire_date").alias("hire_year"),
        F.month("hire_date").alias("hire_month"),
    )
    .agg(
        F.sum(F.when(F.col("level") == "L3", F.col("salary")).otherwise(0)).alias("l3_salary"),
    )
    .orderBy(F.col("hire_year").desc(), "is_remote", "hire_month")
    .limit(50)
)
