from pyspark.sql import functions as F

result = (
    df_departments
    .select("dept_name", F.coalesce("budget", F.lit(0)).alias("budget_filled"))
)
