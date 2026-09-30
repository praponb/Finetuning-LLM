from pyspark.sql import functions as F

result = (
    df_departments
    .filter((F.col("budget") <= 3893000) & ((F.col("budget") < 3005000) | ~F.col("dept_name").isin(["HR", "Marketing"])))
    .select("dept_name", F.round((F.col("budget") * 1.15), 2).alias("budget_adjusted"))
)
