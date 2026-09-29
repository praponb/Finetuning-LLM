from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("signup_date").between("2023-06-20", "2025-08-07"))
    .select("city")
    .intersect(df_suppliers.filter(F.col("country") != "US").select("supplier_name"))
)
