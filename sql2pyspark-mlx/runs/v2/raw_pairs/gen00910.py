from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter((F.col("city").isin(["New York", "Berlin"]) & (F.col("country") == "TH")) | F.col("supplier_name").like("E%"))
    .groupBy("city", "country")
    .agg((F.max("rating") - F.min("rating")).alias("rating_range"))
    .orderBy("city")
)
