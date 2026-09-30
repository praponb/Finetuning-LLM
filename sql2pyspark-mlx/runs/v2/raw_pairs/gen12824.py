from pyspark.sql import functions as F

result = (
    df_departments
    .filter((F.col("dept_name") == "Legal") & F.col("budget").between(1399000, 3632000) & (F.col("dept_name") == "Finance"))
)
