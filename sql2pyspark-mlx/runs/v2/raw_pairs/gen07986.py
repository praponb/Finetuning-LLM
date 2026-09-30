from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy("dept_name")
    .pivot("location", ["London", "Bangkok", "New York"])
    .agg(F.count("budget"))
)
