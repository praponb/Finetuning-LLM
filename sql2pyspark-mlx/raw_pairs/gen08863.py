from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("dept_name").isin(["Marketing", "Engineering", "Sales"]))
    .withColumn("dept_name_location_key", F.concat_ws(",", "dept_name", "location"))
    .withColumn("budget_whole", F.floor("budget"))
)
