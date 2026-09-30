from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("rating") >= 2.2)
    .groupBy("country")
    .pivot("city", ["Chiang Mai", "Tokyo", "New York"])
    .agg(F.sum("rating"))
    .orderBy("country")
)
