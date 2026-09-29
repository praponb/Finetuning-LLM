from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("is_remote"))
    .select("manager_id")
    .unionByName(df_employees.filter(F.col("is_remote")).select("manager_id"))
    .orderBy(F.col("manager_id").desc())
)
