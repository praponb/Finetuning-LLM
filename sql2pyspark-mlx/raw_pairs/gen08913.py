from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("carrier") == "UPS")
    .select("shipped_date")
    .union(df_employees.select("hire_date"))
)
