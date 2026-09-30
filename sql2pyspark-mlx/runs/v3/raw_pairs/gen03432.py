from pyspark.sql import functions as F

result = (
    df_suppliers
    .groupBy(
        "city",
        F.when(F.col("city").isin(["Singapore", "Chiang Mai"]), "core").otherwise("other").alias("city_group"),
    )
    .agg(F.count(F.when(F.col("country") == "UK", 1)).alias("uk_count"))
    .filter(F.col("uk_count") >= 4)
    .select("city", "city_group", "uk_count")
    .orderBy("city_group", "uk_count")
)
