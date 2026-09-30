from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy("location")
    .pivot("dept_name", ["Legal", "Engineering", "Finance", "Sales"])
    .agg(F.count("budget"))
    .orderBy("location")
)
