from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("level") != "L4")
    .select("id", "is_remote")
    .orderBy(F.col("id").desc())
)
