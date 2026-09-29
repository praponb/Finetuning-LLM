from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("shipped_date").isNotNull())
    .select("shipped_date")
    .union(
        df_employees
        .filter(F.col("hire_date").isNotNull())
        .select("hire_date")
    )
    .orderBy("shipped_date")
)
