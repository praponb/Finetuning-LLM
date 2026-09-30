from pyspark.sql import functions as F

result = (
    df_suppliers
    .groupBy("city")
    .pivot("country", ["SG", "US", "DE"])
    .agg(F.avg("rating"))
    .orderBy("city")
)
