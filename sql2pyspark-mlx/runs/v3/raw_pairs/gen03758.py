from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.upper(F.trim("store_name")) == "OLD TOWN")
    .select(
        "sqft",
        "region",
        F.when(F.col("city").isin(["Tokyo", "Chiang Mai"]), "Priority").otherwise("Other").alias("city_group"),
        F.round("sqft", 1).alias("sqft_rounded"),
    )
)
