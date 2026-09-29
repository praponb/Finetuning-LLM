from pyspark.sql import functions as F

result = (
    df_customers
    .select("city", "country")
    .unionByName(df_suppliers.select("city", "country"))
    .distinct()
)
