from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("level") != "L3")
    .groupBy(F.date_format("hire_date", "yyyy-MM").alias("hire_month"))
    .agg(F.count("*").alias("num_employees"))
    .filter(F.col("num_employees") < 10)
    .select("hire_month", "num_employees")
    .orderBy(F.col("num_employees").desc())
)
