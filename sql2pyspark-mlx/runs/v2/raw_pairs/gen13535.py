from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("dept_name").isin(["Sales", "HR"]))
    .withColumn("dept_name_length", F.length("dept_name"))
)
