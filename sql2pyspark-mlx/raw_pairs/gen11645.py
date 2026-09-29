from pyspark.sql import functions as F

result = (
    df_employees
    .filter((F.col("salary") > 139000) & F.col("is_remote"))
    .select("full_name", "salary")
)
