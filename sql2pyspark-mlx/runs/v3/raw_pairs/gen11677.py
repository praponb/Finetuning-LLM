from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("budget") > 1589000)
    .groupBy("location")
    .pivot("dept_name", ["Marketing", "Finance"])
    .agg(F.max("budget"))
)
