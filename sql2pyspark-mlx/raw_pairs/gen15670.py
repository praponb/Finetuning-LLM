from pyspark.sql import functions as F

flattened = df_price_tiers.select("id", "plan", F.explode_outer("discounts").alias("val"))

result = (
    flattened
    .groupBy(F.when(F.col("val") >= 71, "LARGE").otherwise("SMALL").alias("val_tier"))
    .agg(
        F.sort_array(F.collect_set("plan")).alias("plan_list"),
        F.sum(F.when(F.col("plan") == "starter", F.col("val"))).alias("starter_total_val"),
        F.round(F.avg("val"), 2).alias("avg_val"),
    )
    .orderBy(F.col("val_tier").desc_nulls_last())
)
