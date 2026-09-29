from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("job_title") == "Engineer")
    .groupBy("level")
    .agg((F.max("salary") - F.min("salary")).alias("salary_range"))
    .filter(F.col("salary_range") > 65)
    .orderBy(F.col("level").desc())
)
