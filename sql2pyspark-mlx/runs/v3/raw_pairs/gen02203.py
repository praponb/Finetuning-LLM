from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy("dept_name")
    .agg(F.count("*").alias("cnt"))
    .filter(F.col("cnt") > 4)
    .select("dept_name", "cnt")
    .orderBy(F.col("cnt").desc())
)
