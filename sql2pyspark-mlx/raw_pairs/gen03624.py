from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("status") == "DELIVERED")
    .select(F.lit("shipment").alias("origin"), "delivered_date")
    .union(
        df_employees
        .filter(F.col("job_title") != "Intern")
        .select(F.lit("employee").alias("origin"), "hire_date")
    )
    .distinct()
)
