from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.lower("supplier_name").like("%oky%"))
    .groupBy("country")
    .pivot("city", ["London", "Tokyo", "Singapore"])
    .agg(F.avg("rating"))
    .orderBy("country")
)
