from pyspark.sql import functions as F

recent_suppliers = (
    df_suppliers
    .filter(F.col("supplier_name").like("E%") & (F.col("city").isin(["Bangkok", "Tokyo"]) | (F.col("country") != "UK")))
)

summary = (
    recent_suppliers
    .groupBy("city")
    .agg(F.sum("rating").alias("total_rating"))
)

result = summary.orderBy("city", "total_rating")
