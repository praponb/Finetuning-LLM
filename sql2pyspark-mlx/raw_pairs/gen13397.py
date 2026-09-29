from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("manager_id") == 2)
    .select("id", "full_name", "salary")
    .orderBy("full_name", "id")
    .limit(50)
)
