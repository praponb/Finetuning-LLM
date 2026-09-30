from pyspark.sql import functions as F

result = (
    df_employees
    .filter(~F.col("full_name").like("%smi%"))
    .select("salary", "email", "full_name")
    .orderBy(F.col("email").desc())
)
