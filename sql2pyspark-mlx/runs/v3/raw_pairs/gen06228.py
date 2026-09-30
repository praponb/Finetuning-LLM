from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("dept_name") == "HR")
    .groupBy("location")
    .pivot("dept_name", ["Finance", "Sales"])
    .agg(F.max("budget"))
    .orderBy("location")
)
