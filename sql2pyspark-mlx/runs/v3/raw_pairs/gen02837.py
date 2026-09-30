from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("country") == "US")
    .groupBy("country")
    .pivot("city", ["Bangkok", "Berlin"])
    .agg(F.sum("rating"))
)
