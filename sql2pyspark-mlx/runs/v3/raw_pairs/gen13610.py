from pyspark.sql import functions as F

result = (
    df_employees
    .select("hire_date")
    .union(df_products.filter(F.size("tags") >= 1).select("launch_date"))
)
