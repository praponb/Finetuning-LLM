from pyspark.sql import functions as F

result = (
    df_departments
    .withColumn("dept_name_length", F.length("dept_name"))
    .withColumn("budget_adjusted", F.round((F.col("budget") * 100), 2))
)
