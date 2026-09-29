from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter((F.col("rating") > 4.1) & ((F.col("city") == "New York") | (F.length("supplier_name") > 5)))
    .groupBy("city")
    .agg(F.sum(F.when(F.col("city") == "Berlin", 1).otherwise(0)).alias("berlin_count"))
    .filter(F.col("berlin_count") > 49)
    .orderBy(F.col("berlin_count").desc(), "city")
    .limit(5)
)
