from pyspark.sql import functions as F

valid_suppliers = df_suppliers.filter(F.col("country").isin(["US", "TH", "UK"]))

country_summary = (
    valid_suppliers
    .groupBy("country")
    .agg((F.max("rating") - F.min("rating")).alias("rating_range"))
)

result = (
    country_summary
    .select("country", "rating_range")
    .orderBy("rating_range", "country")
    .limit(10)
)
