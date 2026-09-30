from pyspark.sql import functions as F

result = (
    df_inventory
    .select("last_restock")
    .subtract(df_employees.select(F.col("hire_date").alias("last_restock")))
    .orderBy("last_restock")
)
