from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("value")
w2 = Window.partitionBy("customer_age_share_tier").orderBy(F.col("customer_age_share_p50").desc())

lines = (
    df_carts
    .select("id", "customer_id", "created_at", F.explode("coupon_codes").alias("value"))
)

enriched = (
    lines
    .alias("s")
    .join(df_customers.alias("d"), F.col("s.customer_id") == F.col("d.id"), "left")
    .select("s.customer_id", "s.value", F.col("d.age"))
)

enriched_carts = (
    enriched
    .alias("s")
    .join(df_customers.alias("d"), F.col("s.customer_id").eqNullSafe(F.col("d.id")), "left")
    .select(
        "s.*",
        F.col("d.name"),
        F.col("d.age").alias("customer_age"),
        F.col("d.segment"),
    )
)

metrics = (
    enriched_carts
    .filter(F.col("value").isNotNull())
    .select(
        "customer_id",
        "value",
        "name",
        "customer_age",
        "segment",
        F.round((F.col("customer_age") / F.sum("customer_age").over(w)), 4).alias("customer_age_share"),
    )
)

result = (
    metrics
    .groupBy(
        F.when(F.col("customer_age_share") >= 0.7, "PREMIUM").otherwise("STANDARD").alias("customer_age_share_tier"),
        "segment",
        F.when(F.col("customer_age") >= 70, "HIGH").otherwise("LOW").alias("customer_age_tier"),
    )
    .agg(
        F.sort_array(F.collect_set("segment")).alias("segment_list"),
        F.percentile_approx("customer_age_share", 0.5).alias("customer_age_share_p50"),
        F.count_if(F.col("value") == "SPRING10").alias("spring10_events"),
    )
    .filter(F.col("spring10_events") > 1)
    .withColumn("customer_age_share_p50_dense_rank", F.dense_rank().over(w2))
    .orderBy(F.col("segment").desc())
)
