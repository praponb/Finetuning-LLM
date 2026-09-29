from pyspark.sql import functions as F

result = (
    df_inventory
    .select("last_restock")
    .intersect(df_employees.filter(F.col("salary") > 82000).select("hire_date"))
)
