from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("supplier_name").like("%Pac%"))
    .groupBy("country")
    .pivot("city", ["Singapore", "Chiang Mai", "New York"])
    .agg(F.max("rating"))
    .orderBy("country")
)
