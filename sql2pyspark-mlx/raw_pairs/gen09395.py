from pyspark.sql import functions as F

result = (
    df_departments
    .filter((F.col("budget") > 1109000) & (F.col("dept_name") == "Legal"))
    .select("dept_name", F.substring("location", 1, 2).alias("location_prefix"))
    .orderBy("location_prefix")
)
