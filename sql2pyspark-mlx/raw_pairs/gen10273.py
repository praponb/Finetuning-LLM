from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("hire_date").isNotNull())
    .select(F.lit("employee").alias("record_type"), "hire_date")
    .union(
        df_shipments
        .filter(F.col("delivered_date").isNotNull())
        .select(F.lit("shipment").alias("record_type"), "delivered_date")
    )
    .distinct()
)
