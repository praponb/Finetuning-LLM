from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.upper(F.trim("supplier_name")) == "GLOBAL SUPPLY")
    .groupBy("city", "country")
    .agg(F.count(F.when(F.col("city") == "London", 1)).alias("london_count"))
    .filter(F.col("london_count") >= 3)
)
