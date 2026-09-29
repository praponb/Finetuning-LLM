from pyspark.sql import functions as F

result = (
    df_departments
    .filter((F.col("location") != "Chiang Mai") & (F.col("dept_name") == "Sales"))
    .select("location", "budget", "dept_name")
)
