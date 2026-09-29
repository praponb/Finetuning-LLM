from pyspark.sql import functions as F

result = (
    df_departments
    .filter(~F.col("dept_name").isin(["Sales", "Marketing", "Finance"]))
    .select("id", "location", "dept_name")
)
