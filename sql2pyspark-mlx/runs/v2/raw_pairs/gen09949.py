from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("is_active"))
    .select("country", "city")
    .unionByName(df_suppliers.select("country", "city"))
    .distinct()
)
