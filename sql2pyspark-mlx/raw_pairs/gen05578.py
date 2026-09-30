from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("budget").between(1319000, 3849000))
    .select("location")
)
