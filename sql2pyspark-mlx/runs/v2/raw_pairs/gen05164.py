from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.upper("supplier_name").like("%LOB%") | (F.col("city") == "Berlin"))
    .groupBy("city")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("country") == "UK", 1).otherwise(0))) / F.count("*")), 1).alias("pct_uk"),
        F.count("*").alias("cnt"),
    )
    .orderBy("city")
)
