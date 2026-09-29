from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("budget") <= 3008000)
    .groupBy("location")
    .pivot("dept_name", ["Legal", "Sales"])
    .agg(F.max("budget"))
    .orderBy("location")
)
