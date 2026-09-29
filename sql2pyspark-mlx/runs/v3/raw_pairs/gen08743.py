from pyspark.sql import functions as F

result = (
    df_suppliers
    .groupBy("city")
    .pivot("country", ["DE", "US", "JP", "UK"])
    .agg(F.count("rating"))
    .orderBy("city")
)
