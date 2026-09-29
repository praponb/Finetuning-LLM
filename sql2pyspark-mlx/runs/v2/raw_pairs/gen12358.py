from pyspark.sql import functions as F

result = (
    df_departments
    .filter((F.col("location") == "Bangkok") & (F.col("location") == "Berlin"))
    .withColumn("location_group", F.when(F.col("location").isin(["Berlin", "Tokyo"]), "Priority").otherwise("Other"))
    .withColumn("location_prefix", F.substring("location", 1, 2))
    .orderBy("location_group", "id")
    .limit(5)
)
