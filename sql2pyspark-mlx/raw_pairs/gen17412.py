from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("supplier_name").like("%Tra%"))
    .groupBy("country")
    .pivot("city", ["Bangkok", "New York", "Chiang Mai"])
    .agg(F.avg("rating"))
)
