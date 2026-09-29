from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.month("shipped_date").isin([3, 4, 7]))
    .select("shipped_date")
    .subtract(
        df_employees
        .filter(F.col("manager_id").isin([17, 21, 25]))
        .select(F.col("hire_date").alias("shipped_date"))
    )
    .orderBy(F.col("shipped_date").desc())
)
