from pyspark.sql import functions as F

result = (
    df_departments
    .select("dept_name", "budget", F.length("dept_name").alias("dept_name_length"))
)
