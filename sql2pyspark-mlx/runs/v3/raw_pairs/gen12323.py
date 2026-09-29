from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("delivered_date") > "2025-05-08")
    .select("delivered_date")
    .union(df_employees.select("hire_date"))
    .orderBy("delivered_date")
)
