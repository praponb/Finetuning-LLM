from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("supplier_name").like("Eur%") & F.col("country").isin(["DE", "JP"]))
    .groupBy("country")
    .agg(F.sum("rating").alias("total_rating"))
    .filter(F.col("total_rating") >= 14)
    .orderBy("country")
)
