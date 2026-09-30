from pyspark.sql import functions as F

result = (
    df_customers
    .filter(~F.col("is_active"))
    .select("country", "city")
    .intersect(df_suppliers.select("country", "city"))
)
