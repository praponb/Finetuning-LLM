from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("level").isNotNull() & (F.col("level") != "L3") & F.col("job_title").isNotNull())
    .orderBy(F.col("dept_id").desc())
)
