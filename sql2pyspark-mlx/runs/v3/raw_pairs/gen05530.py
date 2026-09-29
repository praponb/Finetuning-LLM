from pyspark.sql import functions as F

result = (
    df_employees
    .groupBy(
        F.quarter("hire_date").alias("hire_quarter"),
        F.year("hire_date").alias("hire_year"),
    )
    .agg(
        F.sum(F.when(F.col("level") == "L3", F.col("salary")).otherwise(0)).alias("l3_salary"),
    )
    .filter(F.col("l3_salary") >= 52)
    .select("hire_quarter", "hire_year", "l3_salary")
    .orderBy(F.col("hire_quarter").desc(), "hire_year")
    .limit(5)
)
