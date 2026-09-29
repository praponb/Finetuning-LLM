from pyspark.sql import functions as F

result = (
    df_suppliers
    .groupBy("country")
    .pivot("city", ["Chiang Mai", "New York", "Tokyo", "Berlin"])
    .agg(F.count("rating"))
    .orderBy("country")
)
