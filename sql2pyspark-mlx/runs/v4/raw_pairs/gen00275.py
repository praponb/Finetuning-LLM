from pyspark.sql import functions as F

result = (
    df_employees
    .select("hire_date")
    .unionByName(
        df_shipments
        .filter(F.col("shipped_date") < "2024-04-01")
        .select(F.col("delivered_date").alias("hire_date"))
    )
    .distinct()
    .orderBy(F.col("hire_date").desc())
)
