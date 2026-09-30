from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("dept_name").isin(["Engineering", "Sales"]) & F.col("dept_name").isNotNull() & F.col("location").isin(["Tokyo", "Bangkok", "Chiang Mai"]))
    .orderBy(F.col("id").desc())
)
