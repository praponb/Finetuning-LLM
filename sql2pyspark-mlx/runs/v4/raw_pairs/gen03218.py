from pyspark.sql import functions as F

result = (
    df_stores
    .select(
        "id",
        "sqft",
        F.when(F.col("region").isin(["East", "South"]), "Group A").otherwise("Other").alias("region_group"),
        F.round("sqft", 1).alias("sqft_rounded"),
    )
)
