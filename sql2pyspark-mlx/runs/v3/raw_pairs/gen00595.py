from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("rating") <= 4.0)
    .groupBy("country")
    .pivot("city", ["Chiang Mai", "Bangkok", "New York", "Singapore"])
    .agg(F.max("rating"))
)
