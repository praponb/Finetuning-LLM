from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("sqft").isNotNull() & ~F.col("store_name").like("%Kio%") & F.col("region").isin(["East", "South"]))
)
