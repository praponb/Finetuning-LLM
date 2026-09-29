from pyspark.sql import functions as F

valid_suppliers = (
    df_suppliers
    .filter((F.col("country") != "JP") & F.col("city").isin(["Berlin", "New York"]) & F.col("city").isin(["Chiang Mai", "Berlin", "Singapore"]))
)

agg = (
    valid_suppliers
    .groupBy("country")
    .agg(
        (F.max("rating") - F.min("rating")).alias("rating_range"),
        F.countDistinct("rating").alias("unique_citys"),
    )
)

result = (
    agg
    .select("country", "rating_range", "unique_citys")
    .orderBy(F.col("rating_range").desc())
)
