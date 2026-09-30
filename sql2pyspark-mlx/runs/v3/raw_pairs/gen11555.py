from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("budget") >= 1909000)
    .select("location")
    .unionByName(df_departments.filter(F.col("budget") >= 1086000).select("location"))
    .distinct()
)
