from pyspark.sql import functions as F

result = (
    df_suppliers
    .groupBy("city")
    .pivot("country", ["JP", "TH", "UK"])
    .agg(F.count("rating"))
)
