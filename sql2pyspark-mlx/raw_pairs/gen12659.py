from pyspark.sql import functions as F

lines = (
    df_carts
    .withColumn("item", F.explode_outer("items"))
    .select(
        "id",
        "created_at",
        F.col("item.product_id"),
        F.col("item.qty"),
        F.col("item.price"),
        F.col("item.promo"),
    )
)

result = (
    lines
    .groupBy("promo")
    .agg(
        F.size(F.collect_set("promo")).alias("num_promos"),
        F.min("created_at").alias("first_seen"),
        ((F.unix_timestamp(F.max("created_at")) - F.unix_timestamp(F.min("created_at"))) / 60).alias("span_minutes"),
        ((F.unix_timestamp(F.max("created_at")) - F.unix_timestamp(F.min("created_at"))) / 60).alias("span_minutes_2"),
    )
    .filter(F.col("span_minutes") >= 0)
)
