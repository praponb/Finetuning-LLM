from pyspark.sql import functions as F

recent_suppliers = (
    df_suppliers
    .filter(~F.col("city").isin(["Singapore", "Tokyo"]) & (F.col("rating") <= 2.3) & (F.col("rating") > 2.6))
)

agg = (
    recent_suppliers
    .groupBy("country", "city")
    .agg(F.sum("rating").alias("total_rating"))
)

result = (
    agg
    .filter(F.col("total_rating") > 12)
    .orderBy(F.col("total_rating").desc())
)
