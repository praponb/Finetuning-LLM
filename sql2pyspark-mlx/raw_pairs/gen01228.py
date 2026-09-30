from pyspark.sql import functions as F

result = (
    df_stores
    .filter(~F.col("city").isin(["Bangkok", "London"]))
    .groupBy(
        F.when(F.col("region").isin(["North", "Central"]), "tier_1").otherwise("other").alias("region_group"),
        "city",
    )
    .agg(F.count(F.when(F.col("region") == "Central", 1)).alias("central_count"))
    .filter(F.col("central_count") < 1)
)
