from pyspark.sql import functions as F

result = (
    df_suppliers
    .groupBy("city")
    .pivot("country", ["SG", "DE", "TH", "UK"])
    .agg(F.sum("rating"))
    .orderBy("city")
)
