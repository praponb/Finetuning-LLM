from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("manager_id").isNotNull())
    .select("id", "full_name", "manager_id")
    .orderBy("full_name", F.col("manager_id").desc(), "id")
    .limit(10)
)
