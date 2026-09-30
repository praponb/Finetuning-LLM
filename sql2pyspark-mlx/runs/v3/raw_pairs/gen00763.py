from pyspark.sql import functions as F

result = (
    df_suppliers
    .groupBy("country", "city")
    .agg((F.max("rating") - F.min("rating")).alias("rating_range"))
    .orderBy("city")
)
