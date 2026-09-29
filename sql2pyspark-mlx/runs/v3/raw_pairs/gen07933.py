from pyspark.sql import functions as F

result = (
    df_departments
    .filter(~((F.col("location") != "Tokyo") & (F.col("budget") >= 2566000) & F.col("location").isNull()))
    .select("dept_name", "location", F.upper("location").alias("location_upper"))
)
