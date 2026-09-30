from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("budget") < 1413000)
    .groupBy("location")
    .pivot("dept_name", ["Legal", "Sales"])
    .agg(F.sum("budget"))
    .orderBy("location")
)
