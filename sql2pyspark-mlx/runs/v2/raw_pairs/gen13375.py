from pyspark.sql import functions as F

result = (
    df_suppliers
    .groupBy("city")
    .pivot("country", ["SG", "DE", "US", "TH"])
    .agg(F.count("rating"))
    .orderBy("city")
)
