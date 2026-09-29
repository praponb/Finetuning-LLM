from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.upper("supplier_name").like("%OKY%"))
    .groupBy("city")
    .agg(F.round(F.avg("rating"), 2).alias("avg_rating"))
    .orderBy("city", F.col("avg_rating").desc())
)
