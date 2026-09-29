from pyspark.sql import functions as F

result = (
    df_customers
    .select("country")
    .unionByName(df_suppliers.filter(F.col("rating") < 3.9).select("country"))
)
