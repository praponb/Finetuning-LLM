from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("carrier") == "FedEx")
    .select("delivered_date")
    .unionByName(df_employees.select(F.col("hire_date").alias("delivered_date")))
)
