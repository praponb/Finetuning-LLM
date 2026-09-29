from pyspark.sql import functions as F

result = (
    df_customers
    .select("city", "country")
    .subtract(df_suppliers.filter(F.col("rating") <= 2.4).select("city", "country"))
    .orderBy("country")
)
