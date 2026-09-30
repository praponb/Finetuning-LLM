from pyspark.sql import functions as F

result = (
    df_departments
    .filter(~F.col("dept_name").isin(["Legal", "Engineering"]))
    .withColumn("is_engineering", F.when(F.col("dept_name") == "Engineering", 1).otherwise(0))
)
