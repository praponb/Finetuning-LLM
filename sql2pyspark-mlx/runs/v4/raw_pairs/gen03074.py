from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("val_tier").orderBy(F.col("eur_count").desc())

exploded = (
    df_price_tiers
    .select("id", "plan", "currency", F.explode_outer("tier_prices").alias("val"))
)

result = (
    exploded
    .groupBy(
        F.when(F.col("val") >= 75, "LARGE").otherwise("SMALL").alias("val_tier"),
        "currency",
    )
    .agg(
        F.sort_array(F.collect_set("currency")).alias("currency_list"),
        F.percentile_approx("val", 0.5).alias("val_p50"),
        F.count(F.when(F.col("currency") == "EUR", 1)).alias("eur_count"),
        F.avg("val").alias("avg_val"),
        F.size(F.collect_set("plan")).alias("num_plans"),
    )
    .withColumn("eur_count_percent_rank", F.percent_rank().over(w))
)
