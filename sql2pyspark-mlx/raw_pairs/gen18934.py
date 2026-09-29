from pyspark.sql import functions as F

result = (
    df_employees
    .filter((F.length("email") > 9) & F.col("full_name").like("%Jack%"))
    .orderBy(F.col("hire_date").desc())
)
