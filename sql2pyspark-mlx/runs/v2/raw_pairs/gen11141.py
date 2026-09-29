from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("age") >= 54)
    .select("city", "country")
    .unionByName(df_suppliers.select("city", "country"))
    .distinct()
)
