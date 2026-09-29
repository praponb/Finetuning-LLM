from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("budget") > 3226000)
    .select("location")
    .distinct()
    .orderBy("location")
)
