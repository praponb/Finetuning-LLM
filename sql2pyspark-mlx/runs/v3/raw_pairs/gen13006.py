from pyspark.sql import functions as F

result = (
    df_suppliers
    .groupBy("city")
    .pivot("country", ["SG", "TH", "DE"])
    .agg(F.count("rating"))
    .orderBy("city")
)
