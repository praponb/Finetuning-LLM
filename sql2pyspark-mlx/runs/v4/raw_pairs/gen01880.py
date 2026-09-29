from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("dept_name").isNull())
    .withColumn("budget_int", F.col("budget").cast("int"))
)
