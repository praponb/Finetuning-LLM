from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("country") != "DE")
    .select("country", "city")
    .unionByName(df_suppliers.select("country", "city"))
    .distinct()
    .orderBy("city")
)
