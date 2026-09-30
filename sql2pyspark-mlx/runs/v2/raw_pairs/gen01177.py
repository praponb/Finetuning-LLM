from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("signup_date").between("2023-12-01", "2025-02-01"))
    .select("id", "segment")
    .unionByName(df_customers.filter(F.col("email").isNull()).select("id", "segment"))
    .distinct()
)
