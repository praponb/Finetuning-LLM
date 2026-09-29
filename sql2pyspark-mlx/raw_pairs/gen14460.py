from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("sqft").between(4531, 5665))
    .select(
        "city",
        "region",
        "store_name",
        F.initcap(F.trim("region")).alias("region_title"),
        F.round((F.col("sqft") * 0.85), 2).alias("sqft_scaled"),
    )
)
