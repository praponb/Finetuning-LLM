from pyspark.sql import functions as F

result = (
    df_departments
    .filter((F.col("dept_name") == "Sales") & F.col("dept_name").isin(["Marketing", "Sales"]))
    .select("id", "budget")
)
