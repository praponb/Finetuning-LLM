from pyspark.sql import functions as F

exploded = df_price_tiers.select("id", F.explode_outer("discounts").alias("val"))

result = (
    exploded
    .groupBy(F.when(F.col("val") >= 86, "HIGH").otherwise("LOW").alias("val_tier"))
    .agg(
        F.count("*").alias("num_rows"),
        F.sum("val").alias("total_val"),
        F.round(F.avg("val"), 2).alias("avg_val"),
    )
    .filter(F.col("num_rows") < 1)
)
