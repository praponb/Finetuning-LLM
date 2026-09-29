from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("level").isNotNull() | (F.length("full_name") >= 14))
    .select("id", "salary", "manager_id")
)
