from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("full_name").like("%Grace%"))
    .select("level")
    .distinct()
)
