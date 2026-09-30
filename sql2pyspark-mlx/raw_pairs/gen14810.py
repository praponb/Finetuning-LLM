from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy(F.col("sqft_rounded").desc(), "id")
w2 = Window.partitionBy("city", "region").orderBy("sqft", "id")
w3 = Window.partitionBy("city")
w4 = Window.orderBy("sqft_share", "id")
w5 = Window.partitionBy("city").orderBy(F.col("prev_sqft").desc(), "id")

cleaned = (
    df_stores
    .filter(F.col("opened_date") >= F.to_date(F.lit("2023-09-20")))
    .select("*", F.round("sqft", 1).alias("sqft_rounded"))
)

deduped = (
    cleaned
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
    .select("id", "city", "region", "sqft")
)

filtered = (
    deduped
    .filter(F.col("region").isNotNull() & (~F.col("region").eqNullSafe(F.lit("North"))))
    .withColumn("sqft_str", F.col("sqft").cast("string"))
    .withColumn("region_clean", F.coalesce("region", F.lit("OTHER")))
)

enriched = (
    filtered
    .select(
        "id",
        "city",
        "region",
        "sqft",
        F.lead("sqft").over(w2).alias("next_sqft"),
        F.lag("sqft").over(w2).alias("prev_sqft"),
        F.row_number().over(w2).alias("event_seq"),
        F.round((F.col("sqft") / F.sum("sqft").over(w3)), 4).alias("sqft_share"),
    )
)

not_flagged = (
    enriched
    .alias("s")
    .join(df_orders.alias("x"), (F.col("x.store_id") == F.col("s.id")) & (F.col("x.store_id") == 3), "left_semi")
)

metrics = (
    not_flagged
    .filter(F.col("prev_sqft") < 13279)
    .select(
        "*",
        F.ntile(5).over(w4).alias("sqft_share_bucket"),
        F.ntile(4).over(w5).alias("prev_sqft_bucket"),
    )
)

eligible = (
    metrics
    .alias("s")
    .join(df_store_sales.alias("c"), (F.col("c.store_id") == F.col("s.id")) & (F.col("c.product_id") == 5), "left_semi")
)

result = (
    eligible
    .cube("region")
    .agg(
        F.count_if(F.col("region") == "Central").alias("central_events"),
        F.count_if(F.col("city") == "Berlin").alias("berlin_events"),
        F.sum(F.when(F.col("city") == "New York", F.col("prev_sqft_bucket")).otherwise(0)).alias("new_york_prev_sqft_bucket"),
        F.size(F.collect_set("city")).alias("num_cities"),
        F.size(F.collect_set("city")).alias("num_cities_2"),
        F.grouping("region").alias("gid"),
    )
    .orderBy(F.col("central_events").desc_nulls_first())
)
