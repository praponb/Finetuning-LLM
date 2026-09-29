from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("is_remote"))
    .select("id", "email", "level")
    .orderBy("email")
)
