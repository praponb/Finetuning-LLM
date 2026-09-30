from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy(F.col("rating").desc(), "id")
w2 = Window.partitionBy("city").orderBy(F.col("rating").desc(), "id")
w3 = Window.partitionBy("city", "country").orderBy("rating", "id")
w_hist = Window.partitionBy("city").orderBy("is_bangkok", "id")
w5 = Window.partitionBy("country")
w6 = Window.orderBy(F.col("is_bangkok_whole").desc())
w7 = Window.partitionBy("city").orderBy("rating_bucket_3", "id")
w8 = Window.partitionBy("city").orderBy("next_is_bangkok", "id")

valid = (
    df_suppliers
    .filter(F.col("city").isNotNull())
    .select(
        "*",
        F.when(F.col("city") == "Bangkok", 1).otherwise(0).alias("is_bangkok"),
        F.coalesce("city", F.lit("N/A")).alias("city_clean"),
    )
)

with_windows = (
    valid
    .withColumn("rating_bucket", F.ntile(4).over(w))
    .withColumn("rating_bucket_2", F.ntile(10).over(w2))
    .withColumn("rating_bucket_3", F.ntile(4).over(w3))
    .withColumn("row_seq", F.row_number().over(w_hist))
    .withColumn("next_is_bangkok", F.lead("is_bangkok", 2, 0).over(w_hist))
)

filtered = (
    with_windows
    .filter(F.col("city").isNotNull() & (F.col("next_is_bangkok") <= 59))
    .select(
        "id",
        "supplier_name",
        "country",
        "city",
        "rating",
        "city_clean",
        "rating_bucket",
        "rating_bucket_2",
        "rating_bucket_3",
        "row_seq",
        "next_is_bangkok",
        F.ceil("is_bangkok").alias("is_bangkok_whole"),
        F.coalesce("city", F.lit("N/A")).alias("city_filled"),
    )
)

enriched = (
    filtered
    .select(
        "*",
        F.min("rating_bucket").over(w5).alias("country_min_rating_bucket"),
        F.rank().over(w6).alias("is_bangkok_whole_rank"),
        F.row_number().over(w7).alias("event_seq"),
    )
)

not_flagged = (
    enriched
    .alias("s")
    .join(df_products.alias("x"), (F.col("x.supplier_id") == F.col("s.id")) & (F.size(F.col("x.tags")) > 2), "left_anti")
)

ranked = (
    not_flagged
    .withColumn("row_num", F.row_number().over(w8))
    .filter(F.col("row_num") == 1)
)

valid_suppliers = (
    ranked
    .filter(F.col("country") == "DE")
    .select(
        "*",
        F.col("next_is_bangkok").cast("string").alias("next_is_bangkok_num"),
        F.coalesce("city", F.lit("DIRECT")).alias("resolved_city"),
    )
)

result = (
    valid_suppliers
    .select(
        "id",
        "supplier_name",
        "country",
        "city",
        "rating",
        "city_clean",
        "rating_bucket_2",
        "rating_bucket_3",
        "row_seq",
    )
)
