from pyspark.sql import functions as F

result = (
    df_departments
    .filter((F.col("dept_name") == "Legal") & F.col("dept_name").isin(["Sales", "Legal", "Finance"]))
    .select("location")
    .orderBy("location")
)
