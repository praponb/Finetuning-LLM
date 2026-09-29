from pyspark.sql import functions as F

result = (
    df_customers
    .select("country", "city")
    .unionByName(df_suppliers.select("country", "city"))
    .distinct()
)
