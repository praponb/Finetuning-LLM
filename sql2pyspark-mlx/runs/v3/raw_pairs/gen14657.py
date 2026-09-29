from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("signup_date") <= "2025-05-01")
    .select("country")
    .intersect(df_suppliers.select("country"))
    .orderBy(F.col("country").desc())
)
