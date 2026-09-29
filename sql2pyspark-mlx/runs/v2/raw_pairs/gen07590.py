from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("rating") <= 2.4)
    .groupBy("country")
    .pivot("city", ["New York", "Singapore"])
    .agg(F.sum("rating"))
    .orderBy("country")
)
