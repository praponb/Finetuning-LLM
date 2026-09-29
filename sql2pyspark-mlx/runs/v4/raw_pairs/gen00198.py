from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("location").isNotNull() & (F.col("dept_name") != "Marketing") & F.col("dept_name").isin(["Legal", "HR"]))
    .select("id", "budget")
    .orderBy(F.col("id").desc())
)
