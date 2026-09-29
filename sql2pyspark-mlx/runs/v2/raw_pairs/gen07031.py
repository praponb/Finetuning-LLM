from pyspark.sql import functions as F

result = (
    df_departments
    .select("budget", F.substring("dept_name", 1, 3).alias("dept_name_prefix"))
)
