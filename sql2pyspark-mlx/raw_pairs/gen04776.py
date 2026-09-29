from pyspark.sql import functions as F

result = (
    df_employees
    .select("full_name")
    .union(
        df_customers
        .filter(F.col("signup_date").between("2023-08-04", "2024-11-21"))
        .select("segment")
    )
)
