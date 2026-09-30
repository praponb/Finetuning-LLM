from pyspark.sql import functions as F

result = (
    df_suppliers
    .groupBy("country")
    .pivot("city", ["Tokyo", "Berlin", "New York", "Singapore"])
    .agg(F.avg("rating"))
    .orderBy("country")
)
