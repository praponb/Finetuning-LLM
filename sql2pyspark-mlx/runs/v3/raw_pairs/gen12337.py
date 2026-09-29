from pyspark.sql import functions as F

result = (
    df_departments
    .filter((F.col("location") == "Berlin") | (F.col("budget") >= 2475000))
    .orderBy("location", "id")
    .limit(10)
)
