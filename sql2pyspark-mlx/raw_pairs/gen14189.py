from pyspark.sql import functions as F

result = (
    df_departments
    .filter((F.col("dept_name") == "HR") | F.col("dept_name").isin(["Sales", "Finance", "Marketing"]))
    .select("id", "budget", "location")
)
