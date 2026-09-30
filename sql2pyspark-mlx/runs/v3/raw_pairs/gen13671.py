from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_stores.alias("s"), F.col("i.store_id") == F.col("s.id"), "left")
    .filter(F.col("i.store_id") == 3)
    .groupBy(
        F.when(F.col("s.city").isin(["Chiang Mai", "Tokyo"]), "tier_1").otherwise("other").alias("city_group"),
        F.col("s.city"),
    )
    .agg(F.count("*").alias("cnt"))
    .orderBy("city_group")
)
