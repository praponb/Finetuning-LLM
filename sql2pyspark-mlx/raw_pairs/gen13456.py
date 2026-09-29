from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("rating").between(2.6, 2.8))
    .groupBy("city")
    .pivot("country", ["SG", "UK", "DE"])
    .agg(F.count("rating"))
    .orderBy("city")
)
