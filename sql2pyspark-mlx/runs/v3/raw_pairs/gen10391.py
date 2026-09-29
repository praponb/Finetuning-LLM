from pyspark.sql import functions as F

result = (
    df_stores
    .rollup(
        "region",
        F.when(F.col("city").isin(["New York", "London"]), "tier_1").otherwise("other").alias("city_group"),
    )
    .agg(F.count(F.when(F.col("city") == "Berlin", 1)).alias("berlin_count"))
)
