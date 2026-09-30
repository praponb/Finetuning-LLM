from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("dept_name") == "Sales")
    .groupBy("location")
    .pivot("dept_name", ["Engineering", "Marketing"])
    .agg(F.count("budget"))
    .orderBy("location")
)
