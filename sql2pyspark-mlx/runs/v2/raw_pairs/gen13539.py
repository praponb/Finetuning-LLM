from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("rating").between(3.4, 3.8))
    .groupBy("city")
    .pivot("country", ["US", "DE", "JP"])
    .agg(F.count("rating"))
)
