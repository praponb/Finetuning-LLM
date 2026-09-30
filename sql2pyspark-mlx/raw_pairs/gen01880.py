from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("location").isin(["Bangkok", "Berlin"]))
    .select("dept_name")
    .distinct()
)
