from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy("location")
    .pivot("dept_name", ["Marketing", "Legal", "Engineering", "Finance"])
    .agg(F.count("budget"))
)
