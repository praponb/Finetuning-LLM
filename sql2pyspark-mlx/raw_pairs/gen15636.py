from pyspark.sql import functions as F

flattened = (
    df_carts
    .withColumn("item", F.explode_outer("items"))
    .select(
        "id",
        "created_at",
        "coupon_codes",
        F.col("item.price"),
        F.col("item.promo"),
    )
)

result = (
    flattened
    .groupBy(F.date_trunc("day", "created_at").alias("activity_day"))
    .agg(
        F.size(F.collect_set("promo")).alias("num_promos"),
        ((F.unix_timestamp(F.max("created_at")) - F.unix_timestamp(F.min("created_at"))) / 60).alias("span_minutes"),
        F.count("*").alias("cnt"),
        F.sort_array(F.collect_set("promo")).alias("promo_list"),
        F.avg("price").alias("avg_price"),
    )
    .orderBy(F.col("num_promos").desc())
)
