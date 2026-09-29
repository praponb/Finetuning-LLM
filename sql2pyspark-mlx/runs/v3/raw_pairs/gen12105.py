from pyspark.sql import functions as F

result = (
    df_employees
    .select("hire_date")
    .unionByName(df_orders.select(F.col("order_date").alias("hire_date")))
)
