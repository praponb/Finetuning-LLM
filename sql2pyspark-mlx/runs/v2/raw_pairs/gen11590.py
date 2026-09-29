from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("level").isin(["L3", "L4", "L2"]) & (F.col("hire_date") < "2025-05-04"))
    .withColumn("is_remote_label", F.when(F.col("is_remote"), "Yes").otherwise("No"))
)
