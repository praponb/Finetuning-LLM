from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("budget") >= 3367000)
    .withColumn("is_hr", F.when(F.col("dept_name") == "HR", 1).otherwise(0))
)
