from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promo").orderBy(F.col("position").desc_nulls_first())

lines = (
    df_carts
    .select("*", F.posexplode("items").alias("pos", "line"))
    .select(
        "id",
        "created_at",
        "coupon_codes",
        (F.col("pos") + 1).alias("position"),
        F.col("line.product_id"),
        F.col("line.price"),
        F.col("line.promo"),
    )
)

valid = (
    lines
    .filter(F.col("promo").isNotNull() & F.col("position").isNull())
    .withColumn("product_id_times_price", (F.col("product_id") * F.col("price")))
    .withColumn("resolved_promo", F.coalesce("promo", F.lit("UNKNOWN")))
)

deduped = (
    valid
    .withColumn("row_num", F.rank().over(w))
    .filter(F.col("row_num") == 1)
)

cleaned = (
    deduped
    .filter(F.col("row_num") <= 1)
    .select(
        "*",
        (F.col("position") * F.col("row_num")).alias("position_times_row_num"),
        F.least("position", "product_id_times_price").alias("least_position_product_id_times_price"),
        F.coalesce("promo", F.lit("N/A")).alias("promo_clean"),
    )
)

totals = (
    cleaned
    .groupBy("promo", "product_id")
    .agg(
        F.countDistinct("promo").alias("distinct_promos"),
        F.count_if(F.col("promo") == "WELCOME5").alias("welcome5_events"),
        F.max("created_at").alias("last_created_at"),
        F.percentile_approx("product_id_times_price", 0.5).alias("product_id_times_price_p50"),
        F.countDistinct("id").alias("distinct_ids"),
    )
)

result = (
    totals
    .select(
        "promo",
        "product_id",
        "welcome5_events",
        "last_created_at",
        "product_id_times_price_p50",
        "distinct_ids",
    )
)
