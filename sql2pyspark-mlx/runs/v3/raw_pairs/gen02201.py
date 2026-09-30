from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("city").isin(["Chiang Mai", "Bangkok", "Tokyo"]))
    .select("country")
    .unionByName(df_suppliers.select("country"))
    .distinct()
)
