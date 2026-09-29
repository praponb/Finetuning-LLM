from pyspark.sql import functions as F

exploded = df_carts.select("id", F.inline("items"))

result = (
    exploded
    .groupBy(
        F.when(F.col("qty") >= 5, "PREMIUM").otherwise("STANDARD").alias("qty_tier"),
    )
    .agg(
        F.count_if(F.col("promo") == "SPRING10").alias("spring10_events"),
        F.size(F.collect_set("promo")).alias("num_promos"),
        F.sum("price").alias("total_price"),
    )
)
