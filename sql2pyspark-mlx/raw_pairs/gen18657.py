from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.month("hire_date").isin([4, 9, 11]) | ~F.col("is_remote"))
    .select("id", "level", "manager_id", "dept_id")
)
