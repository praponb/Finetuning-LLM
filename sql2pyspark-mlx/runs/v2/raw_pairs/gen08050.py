from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("location").isNull())
    .select("dept_name", "location")
    .distinct()
)
