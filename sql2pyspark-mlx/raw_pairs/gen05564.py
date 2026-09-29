from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("store_name").like("Ma%") & F.col("store_name").like("%Airp%"))
    .withColumn("region_group", F.when(F.col("region").isin(["South", "North"]), "Group A").otherwise("Other"))
    .orderBy("city")
)
