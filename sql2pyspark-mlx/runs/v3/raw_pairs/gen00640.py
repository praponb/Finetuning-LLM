from pyspark.sql import functions as F

result = (
    df_employees
    .filter(((F.col("level") == "L3") & (F.upper(F.trim("full_name")) == "IVY CHEN")) | F.lower("full_name").like("%oah%"))
    .select("id", "dept_id", F.length("level").alias("level_length"))
    .orderBy(F.col("id").desc(), F.col("dept_id").desc())
)
