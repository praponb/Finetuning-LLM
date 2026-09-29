from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.price").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("country").orderBy(F.col("rating").desc())
w3 = Window.partitionBy("city").orderBy("rank_in_group", "id")
w4 = Window.partitionBy("country").orderBy(F.col("rating").desc_nulls_last())
w5 = Window.partitionBy("category").orderBy("product_price", "id")

with_last = (
    df_suppliers
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.launch_date").alias("product_launch_date"))
)

latest = (
    with_last
    .withColumn("rank_in_group", F.rank().over(w2))
    .filter(F.col("rank_in_group") == 1)
)

metrics = (
    latest
    .select(
        "id",
        "country",
        "city",
        "rating",
        "product_launch_date",
        "rank_in_group",
        F.lag("rating", 1).over(w3).alias("prev_rating"),
        F.rank().over(w4).alias("rating_rank"),
    )
)

latest_match = (
    metrics
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.country",
        "s.city",
        "s.rating",
        "s.prev_rating",
        "s.rating_rank",
        F.col("c.category"),
        F.col("c.price").alias("product_price"),
    )
)

ranked = (
    latest_match
    .withColumn("rank_in_group", F.row_number().over(w5))
    .filter(F.col("rank_in_group") == 1)
)

result = (
    ranked
    .groupBy(
        F.when(F.col("rating") >= 4.0, "PREMIUM").otherwise("STANDARD").alias("rating_tier"),
    )
    .agg(
        F.count_if(F.col("country") == "DE").alias("de_events"),
        F.count_if(F.col("country") == "SG").alias("sg_events"),
        F.size(F.collect_set("city")).alias("num_cities"),
        F.round(F.sum("rating_rank") / F.nullif(F.count(F.when(F.col("city") == "Chiang Mai", 1)), F.lit(0)), 2).alias("rating_rank_per_chiang_mai"),
    )
    .filter(F.col("sg_events") > 1)
)
