from pyspark.sql import functions as F

not_flagged = (
    df_stores
    .alias("s")
    .join(df_inventory.alias("x"), (F.col("x.store_id") == F.col("s.id")) & (F.col("x.last_restock") <= F.col("s.opened_date")), "left_semi")
)

result = (
    not_flagged
    .groupBy(F.date_trunc("WEEK", "opened_date").alias("week_start"))
    .agg(
        F.stddev_samp("sqft").alias("sqft_stddev"),
        F.round(F.avg("sqft"), 2).alias("avg_sqft"),
        F.size(F.collect_set("region")).alias("num_regions"),
    )
    .orderBy(F.col("sqft_stddev").desc())
)
