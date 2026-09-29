from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("hire_date").isNotNull())
    .select("hire_date")
    .union(df_orders.filter(F.col("order_date").isNotNull()).select("order_date"))
    .distinct()
    .orderBy(F.col("hire_date").desc())
)
