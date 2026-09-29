from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("budget") > 3631000)
    .orderBy("id")
)
