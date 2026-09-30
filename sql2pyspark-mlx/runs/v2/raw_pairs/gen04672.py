from pyspark.sql import functions as F

result = (
    df_customers
    .select("country", "city")
    .intersect(df_suppliers.select("country", "city"))
    .orderBy("city")
)
