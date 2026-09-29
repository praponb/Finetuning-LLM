from pyspark.sql import functions as F

result = (
    df_suppliers
    .groupBy(
        F.when(F.col("country").isin(["SG", "UK"]), "group_a").otherwise("other").alias("country_group"),
    )
    .agg(
        F.round(F.avg("rating"), 2).alias("avg_rating"),
        F.sum(F.when(F.col("country") == "TH", 1).otherwise(0)).alias("th_count"),
    )
    .orderBy("th_count")
)
