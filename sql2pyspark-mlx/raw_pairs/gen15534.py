from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("rating", "id")
w2 = Window.partitionBy("city").orderBy("rating", "id")
w3 = Window.partitionBy("country")
w4 = Window.partitionBy("country").orderBy(F.col("rating").desc(), "id")
w5 = Window.partitionBy("city", "country").orderBy(F.col("rating").desc(), "id")
w6 = Window.partitionBy("country").orderBy("rating_bucket_2", "id")
w_hist = Window.partitionBy("country").orderBy("rank_in_group", "id")

cleaned = (
    df_suppliers
    .filter(F.col("city").isin(["Singapore", "Tokyo"]))
    .select(
        "id",
        "supplier_name",
        "country",
        "city",
        "rating",
        F.initcap(F.trim("city")).alias("city_title"),
        F.coalesce("city", F.lit("UNKNOWN")).alias("city_filled"),
    )
)

scored = (
    cleaned
    .select(
        "*",
        F.lag("rating", 2).over(w).alias("prev_rating"),
        F.ntile(5).over(w2).alias("rating_bucket"),
        F.round((F.col("rating") / F.sum("rating").over(w3)), 4).alias("rating_share"),
        F.ntile(3).over(w4).alias("rating_bucket_2"),
    )
)

deduped = (
    scored
    .withColumn("dedup_rank", F.row_number().over(w5))
    .filter(F.col("dedup_rank") == 1)
    .select(
        "id",
        "supplier_name",
        "country",
        "city_title",
        "prev_rating",
        "rating_bucket",
        "rating_share",
        "rating_bucket_2",
    )
)

deduped_suppliers = (
    deduped
    .withColumn("rank_in_group", F.row_number().over(w6))
    .filter(F.col("rank_in_group") == 1)
)

eligible = (
    deduped_suppliers
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")) & (F.col("c.brand").isin(["Acme", "Stark", "Umbrella"])), "left_anti")
)

metrics = (
    eligible
    .filter(F.col("rating_bucket") >= 4)
    .select(
        "id",
        "supplier_name",
        "city_title",
        "prev_rating",
        "rating_bucket",
        "rank_in_group",
        F.lag("rating_share").over(w_hist).alias("prev_rating_share"),
        F.lag("prev_rating").over(w_hist).alias("prev_prev_rating"),
        F.row_number().over(w_hist).alias("event_seq"),
    )
)

stats = (
    metrics
    .groupBy(
        F.when(F.col("prev_rating_share") >= 0.85, "HIGH").otherwise("LOW").alias("prev_rating_share_tier"),
    )
    .agg(
        F.round(F.avg("rating_bucket"), 2).alias("avg_rating_bucket"),
        F.countDistinct("id").alias("distinct_ids"),
        F.countDistinct("id").alias("distinct_ids_2"),
        F.percentile_approx("prev_rating", [0.25, 0.5, 0.75]).alias("prev_rating_quartiles"),
        F.round(F.avg("prev_rating"), 2).alias("avg_prev_rating"),
    )
)

result = (
    stats
    .select(
        "prev_rating_share_tier",
        "avg_rating_bucket",
        "distinct_ids_2",
        "avg_prev_rating",
    )
)
