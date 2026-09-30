from pyspark.sql import functions as F

filtered_employees = (
    df_employees
    .filter(~((F.col("hire_date") > "2024-06-01") | F.col("job_title").isin(["Manager", "Designer", "Analyst"])))
)

agg = (
    filtered_employees
    .groupBy(F.month("hire_date").alias("hire_month"))
    .agg(
        F.sum(F.when(F.col("level") == "L1", 1).otherwise(0)).alias("l1_count"),
        F.min("hire_date").alias("first_hire_date"),
    )
)

result = (
    agg
    .filter(F.col("l1_count") < 37)
    .select("hire_month", "l1_count", "first_hire_date")
    .orderBy(F.col("l1_count").desc(), "hire_month")
    .limit(5)
)
