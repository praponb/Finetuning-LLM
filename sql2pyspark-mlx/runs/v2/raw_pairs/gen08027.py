from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("name").isNull())
    .select("city", "country")
    .intersect(df_suppliers.select("city", "country"))
)
