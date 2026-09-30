from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("city").isin(["Berlin", "Chiang Mai", "Singapore"]))
    .groupBy("city")
    .pivot("country", ["DE", "UK", "US"])
    .agg(F.count("rating"))
)
