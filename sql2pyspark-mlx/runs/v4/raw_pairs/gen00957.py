from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("country") == "JP")
    .select("country")
    .intersect(df_suppliers.filter(F.col("rating") < 2.0).select("country"))
    .orderBy("country")
)
