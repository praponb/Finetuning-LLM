from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("budget") >= 3381000)
    .select("location")
    .unionByName(df_departments.filter(F.col("budget") >= 3683000).select("location"))
    .distinct()
)
