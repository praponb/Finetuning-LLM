from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("name").like("P%"))
    .select("country", "city")
    .unionByName(df_suppliers.select("country", "city"))
    .distinct()
)
