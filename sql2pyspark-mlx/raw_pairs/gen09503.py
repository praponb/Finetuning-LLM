from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.units").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("region")
w3 = Window.partitionBy("store_sale_units_share_tier").orderBy(F.col("distinct_cities").desc())

with_latest = (
    df_stores
    .alias("s")
    .join(df_store_sales.alias("c"), (F.col("c.store_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.units").alias("store_sale_units"))
)

kept_keys = (
    with_latest
    .select("city", "region")
    .subtract(
        df_stores
        .filter(F.col("store_name").like("%Kios%"))
        .select("city", "region")
    )
)

reconciled = (
    with_latest
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.city") == F.col("k.city")) & (F.col("s.region") == F.col("k.region")), "inner")
    .select("s.*")
)

enriched = (
    reconciled
    .withColumn("store_sale_units_share", F.round((F.col("store_sale_units") / F.sum("store_sale_units").over(w2)), 4))
    .withColumn("region_avg_sqft", F.avg("sqft").over(w2))
)

not_flagged = (
    enriched
    .alias("s")
    .join(df_inventory.alias("x"), (F.col("x.store_id") == F.col("s.id")) & (F.col("x.quantity") <= 221), "left_anti")
)

result = (
    not_flagged
    .groupBy(
        F.when(F.col("store_sale_units_share") >= 0.78, "PREMIUM").otherwise("STANDARD").alias("store_sale_units_share_tier"),
        F.date_trunc("WEEK", "opened_date").alias("week_start"),
    )
    .agg(
        F.percentile_approx("sqft", [0.25, 0.5, 0.75]).alias("sqft_quartiles"),
        F.sum(F.when(F.col("region") == "Central", F.col("sqft")).otherwise(0)).alias("central_sqft"),
        F.percentile_approx("sqft", [0.25, 0.5, 0.75]).alias("sqft_quartiles_2"),
        F.sort_array(F.collect_set("city")).alias("city_list"),
        F.countDistinct("city").alias("distinct_cities"),
    )
    .withColumn("distinct_cities_dense_rank", F.dense_rank().over(w3))
)
