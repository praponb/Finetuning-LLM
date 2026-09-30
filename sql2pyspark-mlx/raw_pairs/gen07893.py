from pyspark.sql import functions as F

result = (
    df_departments
    .select("location")
    .distinct()
)
