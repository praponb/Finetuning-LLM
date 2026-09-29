from pyspark.sql import functions as F

src = (
    df_employees
    .filter((F.col("hire_date") > "2024-06-25") & F.col("email").isNotNull())
)

level_summary = (
    src
    .groupBy("level")
    .agg(F.avg("salary").alias("avg_salary"))
)

result = level_summary.filter(F.col("avg_salary") > 71000)
