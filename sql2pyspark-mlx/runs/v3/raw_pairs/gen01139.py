from pyspark.sql import functions as F

result = (
    df_suppliers
    .groupBy("city")
    .pivot("country", ["SG", "UK", "TH"])
    .agg(F.max("rating"))
    .orderBy("city")
)
