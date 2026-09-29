from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("dept_id").isin([21, 33, 34]))
    .select("id", "salary")
)
