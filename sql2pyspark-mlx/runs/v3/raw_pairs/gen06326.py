from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy(
        "region",
        F.when(F.col("region").isin(["Central", "West"]), "tier_1").otherwise("other").alias("region_group"),
        F.year("opened_date").alias("opened_year"),
    )
    .agg((F.max("sqft") - F.min("sqft")).alias("sqft_range"))
    .filter(F.col("sqft_range") >= 63)
)
