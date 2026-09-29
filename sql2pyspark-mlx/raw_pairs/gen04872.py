from pyspark.sql import functions as F

lines = (
    df_carts
    .select("*", F.posexplode("items").alias("pos", "rec"))
    .select(
        "id",
        "customer_id",
        "created_at",
        "coupon_codes",
        (F.col("pos") + 1).alias("item_no"),
        F.col("rec.qty"),
        F.col("rec.price"),
        F.col("rec.promo"),
    )
)

result = (
    lines
    .withColumn("hour_window", F.date_trunc("HOUR", "created_at"))
    .groupingSets(
        [["hour_window", "item_no"], ["hour_window"], []],
        "hour_window",
        "item_no",
    )
    .agg(
        F.sort_array(F.collect_set("promo")).alias("promo_list"),
        F.sum(F.when(F.col("promo") == "BUNDLE", F.col("qty")).otherwise(0)).alias("bundle_qty"),
        F.round(F.avg("item_no"), 2).alias("avg_item_no"),
        F.grouping("hour_window").alias("grouping_id"),
    )
    .orderBy(F.col("item_no").desc_nulls_first())
)
