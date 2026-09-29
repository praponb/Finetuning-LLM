from pyspark.sql import functions as F

lines = df_price_tiers.select("id", F.explode_outer("discounts").alias("element"))

result = (
    lines
    .groupBy(
        F.when(F.col("element") >= 76, "HIGH").otherwise("LOW").alias("element_tier"),
    )
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.count("*").alias("cnt"),
        F.countDistinct("id").alias("distinct_ids_2"),
        F.round(F.avg("element"), 2).alias("avg_element"),
    )
)
