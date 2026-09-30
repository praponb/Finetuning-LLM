from pyspark.sql import functions as F

result = (
    df_shipments
    .select(
        F.col("shipped_date").alias("event_date"),
        F.lit("shipment").alias("origin"),
    )
    .unionByName(
        df_employees
        .select(F.col("hire_date").alias("event_date"), F.lit("employee").alias("origin"))
    )
)
