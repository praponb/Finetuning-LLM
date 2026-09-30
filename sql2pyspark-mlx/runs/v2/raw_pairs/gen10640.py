from pyspark.sql import functions as F

result = (
    df_employees
    .groupBy(F.date_format("hire_date", "yyyy-MM").alias("hire_month"))
    .agg(
        F.max("hire_date").alias("latest_hire_date"),
        F.count(F.when(F.col("level") == "L4", 1)).alias("l4_count"),
        F.sum("salary").alias("total_salary"),
    )
    .filter(F.col("total_salary") > 813000)
    .orderBy(F.col("l4_count").desc(), "hire_month")
    .limit(1)
)
