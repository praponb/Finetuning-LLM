from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promo")

exploded = (
    df_carts
    .select("*", F.posexplode_outer("items").alias("pos", "item"))
    .select(
        "id",
        "customer_id",
        "created_at",
        "coupon_codes",
        "pos",
        F.col("item.product_id"),
        F.col("item.qty"),
        F.col("item.price"),
        F.col("item.promo"),
    )
)

lines = (
    exploded
    .select(
        "id",
        "customer_id",
        "created_at",
        "promo",
        F.explode_outer("coupon_codes").alias("tag"),
    )
)

valid = (
    lines
    .filter((F.col("created_at") >= F.lit("2025-03-02").cast("date")) & F.col("promo").isNotNull())
    .select(
        "promo",
        F.quarter("created_at").alias("created_at_quarter"),
        F.initcap(F.trim("tag")).alias("tag_title"),
        F.coalesce("tag", F.lit("OTHER")).alias("resolved_tag"),
    )
)

metrics = (
    valid
    .withColumn("promo_stddev_samp_created_at_quarter", F.stddev_samp("created_at_quarter").over(w))
)

result = (
    metrics
    .groupBy(
        F.when(F.col("promo_stddev_samp_created_at_quarter") >= 82, "LARGE").otherwise("SMALL").alias("promo_stddev_samp_created_at_quarter_tier"),
    )
    .agg(
        F.sum(F.when(F.col("promo") == "SPRING10", F.col("promo_stddev_samp_created_at_quarter")).otherwise(0)).alias("spring10_promo_stddev_samp_created_at_quarter"),
        F.size(F.collect_set("promo")).alias("num_promos"),
    )
)
