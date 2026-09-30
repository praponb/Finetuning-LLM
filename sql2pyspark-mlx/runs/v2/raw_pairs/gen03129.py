from pyspark.sql import functions as F

result = (
    df_employees
    .filter((F.col("level") == "L4") & (F.col("job_title") != "Designer"))
    .orderBy(F.col("salary").desc(), "id")
    .limit(5)
)
