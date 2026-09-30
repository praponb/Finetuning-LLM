from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promo")
w2 = Window.orderBy(F.col("created_at").cast("bigint")).rangeBetween(-7200, Window.currentRow)
w_recent = Window.partitionBy("promo").orderBy(F.col("created_at").cast("bigint")).rangeBetween(-7200, Window.currentRow)

lines = (
    df_carts
    .withColumn("rec", F.explode("items"))
    .select(
        "id",
        "created_at",
        F.col("rec.product_id"),
        F.col("rec.qty"),
        F.col("rec.price"),
        F.col("rec.promo"),
    )
)

enriched = lines.withColumn("promo_max_price", F.max("price").over(w))

cleaned = (
    enriched
    .filter((F.col("created_at") > F.lit("2025-03-01").cast("date")) & (F.col("promo_max_price") > 235))
    .select("id", "created_at", "price", "promo", F.trim("promo").alias("clean_promo"))
)

metrics = (
    cleaned
    .select(
        "*",
        F.max("price").over(w2).alias("price_max_2h"),
        F.count("price").over(w_recent).alias("price_count_2h"),
    )
)

result = (
    metrics
    .withColumn("activity_day", F.date_trunc("DAY", "created_at"))
    .withColumn("price_count_2h_tier", F.when(F.col("price_count_2h") >= 13, "PREMIUM").otherwise("STANDARD"))
    .groupingSets(
        [["activity_day", "price_count_2h_tier"], ["activity_day"]],
        "activity_day",
        "price_count_2h_tier",
    )
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.round(F.avg("price"), 2).alias("avg_price"),
        F.sum("price_max_2h").alias("total_price_max_2h"),
        F.size(F.collect_set("promo")).alias("num_promos"),
        F.grouping_id().alias("grouping_level"),
    )
)
