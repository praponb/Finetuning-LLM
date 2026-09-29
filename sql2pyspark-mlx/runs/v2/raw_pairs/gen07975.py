from pyspark.sql import functions as F

result = (
    df_suppliers
    .groupBy("country")
    .pivot("city", ["Chiang Mai", "Singapore", "London", "Berlin"])
    .agg(F.sum("rating"))
)
