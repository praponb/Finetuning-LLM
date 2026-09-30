from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("location") == "London")
    .groupBy("location")
    .pivot("dept_name", ["HR", "Sales"])
    .agg(F.count("budget"))
    .orderBy("location")
)
