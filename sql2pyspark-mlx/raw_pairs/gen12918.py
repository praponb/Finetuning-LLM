from pyspark.sql import functions as F

result = (
    df_suppliers
    .groupBy("city")
    .pivot("country", ["SG", "TH", "US"])
    .agg(F.count("rating"))
)
