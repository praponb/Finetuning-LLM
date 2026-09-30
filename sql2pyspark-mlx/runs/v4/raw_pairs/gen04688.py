from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("val_tier").orderBy(F.col("avg_val_2").desc())

exploded = df_price_tiers.select("id", "currency", F.explode_outer("tier_prices").alias("val"))

result = (
    exploded
    .groupBy(
        F.when(F.col("val") >= 66, "LARGE").otherwise("SMALL").alias("val_tier"),
        "currency",
    )
    .agg(
        F.count(F.when(F.col("currency") == "USD", 1)).alias("usd_count"),
        F.round(F.avg("val"), 2).alias("avg_val"),
        F.round(F.avg("val"), 2).alias("avg_val_2"),
        F.countDistinct("id").alias("distinct_ids"),
    )
    .filter(F.col("usd_count") >= 1)
    .withColumn("avg_val_2_percent_rank", F.percent_rank().over(w))
    .orderBy(F.col("avg_val_2").desc_nulls_last())
)
