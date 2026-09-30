from pyspark.sql import functions as F

result = (
    df_departments
    .select("dept_name", F.round((F.col("budget") * 1.07), 2).alias("budget_scaled"))
)
