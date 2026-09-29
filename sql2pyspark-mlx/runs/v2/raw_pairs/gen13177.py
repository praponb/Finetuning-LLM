from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("budget") >= 1499000)
    .groupBy("location")
    .pivot("dept_name", ["Engineering", "Sales"])
    .agg(F.sum("budget"))
)
