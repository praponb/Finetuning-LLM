from pyspark.sql import functions as F

lines = df_carts.select("id", F.explode("attributes").alias("attr_key", "value"))

result = (
    lines
    .groupBy("attr_key")
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.count(F.when(F.col("attr_key") == "utm_source", 1)).alias("utm_source_count"),
    )
    .orderBy("attr_key")
)
