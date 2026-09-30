from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("qty").desc_nulls_last())
w2 = Window.partitionBy("promo").orderBy(F.col("qty_str").desc_nulls_last())
w3 = Window.partitionBy("promo")

flattened = (
    df_carts
    .withColumn("elem", F.explode("items"))
    .select(
        "id",
        "created_at",
        "coupon_codes",
        F.col("elem.product_id"),
        F.col("elem.qty"),
        F.col("elem.promo"),
    )
)

filtered = (
    flattened
    .filter(F.col("promo").isNotNull())
    .select(
        "qty",
        "promo",
        F.to_date("created_at").alias("created_at_date"),
        F.col("qty").cast("double").alias("qty_str"),
    )
)

scored = (
    filtered
    .withColumn("qty_cume_dist", F.cume_dist().over(w))
    .withColumn("qty_str_cume_dist", F.cume_dist().over(w2))
    .withColumn("qty_share", F.round((F.col("qty") / F.sum("qty").over(w3)), 4))
)

enriched = (
    scored
    .withColumn("promo_min_qty_cume_dist", F.min("qty_cume_dist").over(w3))
    .withColumn("qty_str_cume_dist_2", F.cume_dist().over(w2))
    .withColumn("promo_stddev_samp_qty", F.stddev_samp("qty").over(w3))
)

result = (
    enriched
    .groupBy(F.date_trunc("month", "created_at_date").alias("activity_month"))
    .agg(
        F.max("promo_min_qty_cume_dist").alias("max_promo_min_qty_cume_dist"),
        F.size(F.collect_set("promo")).alias("num_promos"),
        F.sort_array(F.collect_set("promo")).alias("promo_list"),
        F.count("*").alias("event_count"),
    )
    .filter(F.col("num_promos") < 1)
    .orderBy("activity_month")
)
