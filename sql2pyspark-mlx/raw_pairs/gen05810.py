from pyspark.sql import functions as F

result = (
    df_employees
    .select("is_remote", "email", "manager_id")
    .orderBy(F.col("manager_id").desc())
)
