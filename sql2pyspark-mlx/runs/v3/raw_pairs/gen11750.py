from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("region").isin(["East", "Central", "North"]))
    .select(
        "region",
        F.when(F.col("city").isin(["Chiang Mai", "Singapore"]), "Group A").otherwise("Other").alias("city_group"),
    )
    .orderBy("city_group")
)
