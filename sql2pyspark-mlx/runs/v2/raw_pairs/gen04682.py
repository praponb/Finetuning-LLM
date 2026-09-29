from pyspark.sql import functions as F

result = (
    df_stores
    .filter((F.col("region") == "West") & F.col("region").isin(["East", "North", "South"]))
    .withColumn("region_lower", F.lower("region"))
    .withColumn("is_central", F.when(F.col("region") == "Central", 1).otherwise(0))
)
