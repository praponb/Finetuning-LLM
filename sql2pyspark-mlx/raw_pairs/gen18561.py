from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.to_date("created_at"), "promo").orderBy(F.col("price").desc())
w2 = Window.partitionBy("promo")
w3 = Window.orderBy(F.col("created_at").cast("bigint")).rangeBetween(-3600, Window.currentRow)
w4 = Window.partitionBy("promo").orderBy(F.col("promo_max_product_id").desc())
w5 = Window.partitionBy("promo").orderBy(F.col("product_id").desc_nulls_last())

flattened = (
    df_carts
    .withColumn("line", F.explode("items"))
    .select(
        "id",
        "created_at",
        F.col("line.product_id"),
        F.col("line.price"),
        F.col("line.promo"),
    )
)

enriched = (
    flattened
    .withColumn("price_percent_rank", F.percent_rank().over(w))
    .withColumn("promo_max_product_id", F.max("product_id").over(w2))
    .withColumn("product_id_avg_1h", F.avg("product_id").over(w3))
)

cleaned = (
    enriched
    .filter((F.col("created_at") >= F.lit("2025-03-01 12:00:00").cast("timestamp")) & F.col("promo").isNotNull())
    .withColumn("price_adjusted", F.round((F.col("price") * 100), 2))
)

base = (
    cleaned
    .filter(~F.col("promo").eqNullSafe(F.lit("SPRING10")))
    .withColumn("promo_max_product_id_str", F.col("promo_max_product_id").cast("string"))
    .withColumn("promo_clean", F.coalesce("promo", F.lit("N/A")))
)

scored = (
    base
    .withColumn("promo_max_product_id_percent_rank", F.percent_rank().over(w4))
    .withColumn("promo_stddev_samp_price", F.stddev_samp("price").over(w2))
    .withColumn("product_id_cume_dist", F.cume_dist().over(w5))
)

result = (
    scored
    .select(
        "id",
        "created_at",
        "product_id",
        "promo",
        "promo_max_product_id",
        "product_id_avg_1h",
        "price_adjusted",
        "promo_clean",
        "promo_max_product_id_percent_rank",
        F.when(F.col("promo").rlike("^(VIP|BUNDLE)$"), "GROUP_B").when(F.col("promo").rlike("^(SPRING10|WELCOME5)$"), "EXTENDED").otherwise("GROUP_A").alias("promo_zone"),
    )
    .orderBy(F.col("promo_zone").desc_nulls_first())
)
