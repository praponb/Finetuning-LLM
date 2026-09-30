from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter((F.col("city") == "Berlin") & (F.col("rating") >= 3.3))
    .groupBy("city")
    .agg((F.max("rating") - F.min("rating")).alias("rating_range"))
    .orderBy("city")
)
