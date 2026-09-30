from pyspark.sql import functions as F

result = (
    df_employees
    .select("hire_date")
    .union(df_inventory.select("last_restock"))
    .distinct()
)
