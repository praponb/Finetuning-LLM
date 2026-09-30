from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("manager_id") == 32)
    .select("email")
    .orderBy("email")
)
