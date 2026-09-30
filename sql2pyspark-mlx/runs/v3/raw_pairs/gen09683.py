from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("country").isin(["US", "SG"]) & ~F.col("supplier_name").like("%Pacific%"))
    .groupBy("city")
    .agg(F.max("rating").alias("max_rating"))
    .filter(F.col("max_rating") >= 4.2)
)
