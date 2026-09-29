from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("level").isin(["L3", "L5"]))
    .select("id", "email", "manager_id")
    .orderBy("manager_id", "email", "id")
    .limit(1)
)
