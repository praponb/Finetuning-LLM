from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("name").like("%Grace%"))
    .select("country", "city")
    .subtract(df_suppliers.select("country", "city"))
)
