from pyspark.sql import functions as F

result = (
    df_departments
    .filter((F.col("location") == "Singapore") & (F.col("dept_name") == "Legal"))
    .select("id", "location", F.coalesce("budget", F.lit(0)).alias("budget_filled"))
)
