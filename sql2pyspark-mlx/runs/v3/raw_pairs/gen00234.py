from pyspark.sql import functions as F

result = (
    df_suppliers
    .groupBy("country")
    .pivot("city", ["Chiang Mai", "Berlin", "Bangkok", "London"])
    .agg(F.count("rating"))
)
