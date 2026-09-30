from pyspark.sql import functions as F

result = (
    df_suppliers
    .groupBy("city")
    .pivot("country", ["US", "DE", "TH", "UK"])
    .agg(F.avg("rating"))
    .orderBy("city")
)
