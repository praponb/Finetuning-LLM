from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_stores.alias("s"), F.col("s.id") == F.col("i.store_id"), "left")
    .groupBy(
        F.col("s.region"),
        F.when(F.col("s.city").isin(["Berlin", "New York"]), "group_a").otherwise("other").alias("city_group"),
    )
    .agg(
        F.countDistinct(F.col("i.quantity")).alias("unique_stores"),
        F.count("*").alias("cnt"),
    )
    .orderBy("region", "city_group")
)
