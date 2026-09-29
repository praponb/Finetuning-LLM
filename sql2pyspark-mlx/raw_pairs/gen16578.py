from pyspark.sql import functions as F

exploded = df_price_tiers.select("id", "plan", "currency", F.explode("discounts").alias("val"))

result = (
    exploded
    .groupBy("plan")
    .agg(
        F.round(F.avg("val"), 2).alias("avg_val"),
        F.sort_array(F.collect_set("currency")).alias("currency_list"),
    )
    .orderBy(F.col("avg_val").asc_nulls_last())
)
