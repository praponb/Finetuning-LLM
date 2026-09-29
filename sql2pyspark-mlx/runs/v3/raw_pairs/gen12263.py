from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("city").isNull() & (F.col("country") == "UK"))
    .select(
        "id",
        "country",
        F.ceil("rating").alias("rating_whole"),
        F.when(F.col("city").isin(["Berlin", "Chiang Mai"]), "Priority").otherwise("Other").alias("city_group"),
    )
    .orderBy("city_group", F.col("id").desc())
    .limit(3)
)
