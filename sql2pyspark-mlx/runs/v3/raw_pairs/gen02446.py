from pyspark.sql import functions as F

result = (
    df_customers
    .select("country")
    .unionByName(df_suppliers.filter(F.col("city") == "Berlin").select("country"))
    .orderBy(F.col("country").desc())
)
