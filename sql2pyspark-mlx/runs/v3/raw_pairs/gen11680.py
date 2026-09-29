from pyspark.sql import functions as F

result = (
    df_employees
    .filter((F.length("full_name") > 14) & (F.col("salary") <= 61000))
    .select("id", "salary", "level", "full_name")
)
