from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("manager_id").isin([10, 21, 38]) & (F.col("manager_id") == 8))
    .select("id", "manager_id")
    .orderBy(F.col("manager_id").desc())
)
