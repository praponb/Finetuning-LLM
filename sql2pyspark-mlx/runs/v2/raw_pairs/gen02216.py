from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("rating") < 4.2)
    .groupBy("city")
    .pivot("country", ["SG", "DE", "US"])
    .agg(F.max("rating"))
    .orderBy("city")
)
