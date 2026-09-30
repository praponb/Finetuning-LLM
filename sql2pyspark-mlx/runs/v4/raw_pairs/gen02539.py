from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("shipped_date") >= "2025-09-11")
    .select("shipped_date")
    .subtract(
        df_customers
        .filter(F.col("signup_date").between("2025-08-16", "2025-10-01"))
        .select("signup_date")
    )
)
