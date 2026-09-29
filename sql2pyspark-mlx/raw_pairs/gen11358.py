from pyspark.sql import functions as F

flattened = df_price_tiers.select("id", "currency", F.explode_outer("discounts").alias("val"))

result = (
    flattened
    .withColumn("val_tier", F.when(F.col("val") >= 67, "LARGE").otherwise("SMALL"))
    .groupingSets([["val_tier", "currency"], ["val_tier"]], "val_tier", "currency")
    .agg(
        F.round(F.sum("val") / F.nullif(F.count(F.when(F.col("currency") == "THB", 1)), F.lit(0)), 2).alias("val_per_thb"),
        F.count("*").alias("event_count"),
        F.countDistinct("id").alias("distinct_ids"),
        F.countDistinct("id").alias("distinct_ids_2"),
        F.sort_array(F.collect_set("currency")).alias("currency_list"),
    )
    .orderBy(F.col("val_per_thb").desc())
)
