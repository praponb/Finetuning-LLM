from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy("location")
    .pivot("dept_name", ["Sales", "HR", "Engineering"])
    .agg(F.sum("budget"))
    .orderBy("location")
)
