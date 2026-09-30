from pyspark.sql import functions as F

flattened = df_price_tiers.select("id", "currency", F.explode("tier_limits").alias("element"))

result = (
    flattened
    .groupBy(
        F.when(F.col("element") >= 79, "LARGE").otherwise("SMALL").alias("element_tier"),
        "currency",
    )
    .agg(
        F.countDistinct("currency").alias("distinct_currencies"),
        F.size(F.collect_set("currency")).alias("num_currencies"),
        F.round(F.avg("element"), 2).alias("avg_element"),
        F.sort_array(F.collect_set("currency")).alias("currency_list"),
        F.count("*").alias("cnt"),
        F.count_if(F.col("currency") == "EUR").alias("eur_events"),
    )
    .filter(F.col("num_currencies") > 1)
)
