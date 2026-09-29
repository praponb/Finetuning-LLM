from pyspark.sql import functions as F

result = (
    df_shipments
    .select("shipped_date", F.lit("shipment").alias("source"))
    .union(
        df_employees
        .filter(F.col("manager_id").isin([15, 28, 36, 40]))
        .select("hire_date", F.lit("employee").alias("source"))
    )
    .orderBy(F.col("shipped_date").desc())
)
