from pyspark.sql import functions as F

base = df_suppliers.filter(F.col("city") == "New York")

agg = (
    base
    .groupBy(
        F.when(F.col("country").isin(["JP", "DE"]), "group_a").otherwise("other").alias("country_group"),
    )
    .agg(
        F.round(F.avg("rating"), 2).alias("avg_rating"),
        F.min("rating").alias("min_rating"),
    )
)

result = (
    agg
    .select("country_group", "min_rating")
    .orderBy("country_group")
)
