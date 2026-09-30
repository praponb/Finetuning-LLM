from pyspark.sql import functions as F

valid_stores = df_stores.filter((F.col("region") == "West") & F.upper("store_name").like("%ALL%"))

agg = (
    valid_stores
    .groupBy(
        F.when(F.col("region").isin(["East", "South"]), "core").otherwise("other").alias("region_group"),
        "city",
    )
    .agg(F.countDistinct("sqft").alias("unique_regions"))
)

result = (
    agg
    .select("region_group", "city", "unique_regions")
    .orderBy("unique_regions")
)
