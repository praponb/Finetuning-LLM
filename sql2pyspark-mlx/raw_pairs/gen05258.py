from pyspark.sql import functions as F

result = (
    df_stores
    .select(
        "id",
        "region",
        F.col("sqft").cast("string").alias("sqft_str"),
        F.initcap(F.trim("region")).alias("region_title"),
    )
    .orderBy(F.col("id").desc(), "region_title")
)
