from pyspark.sql import functions as F

result = (
    df_inventory
    .select("last_restock")
    .subtract(df_employees.select("hire_date"))
    .orderBy("last_restock")
)
