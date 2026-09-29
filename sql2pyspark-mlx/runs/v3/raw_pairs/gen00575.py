from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("level") == "L1")
    .groupBy(F.year("hire_date").alias("hire_year"))
    .agg(
        F.sum(F.when(F.col("level") == "L5", F.col("salary")).otherwise(0)).alias("l5_salary"),
        F.max("hire_date").alias("latest_hire_date"),
        F.min("hire_date").alias("first_hire_date"),
    )
    .orderBy(F.col("latest_hire_date").desc())
)
