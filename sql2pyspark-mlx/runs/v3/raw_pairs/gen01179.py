from pyspark.sql import functions as F

result = (
    df_products
    .select("launch_date")
    .intersect(df_employees.select("hire_date"))
)
