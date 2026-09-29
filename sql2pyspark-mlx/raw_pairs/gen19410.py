from pyspark.sql import functions as F

result = (
    df_employees
    .select("hire_date")
    .union(
        df_shipments
        .filter(F.col("delivered_date").between("2024-01-01", "2024-10-01"))
        .select("delivered_date")
    )
    .orderBy("hire_date")
)
