from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.cost").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("country").orderBy(F.col("product_price").asc_nulls_first())
w3 = Window.partitionBy("city").orderBy("rating", "id").rowsBetween(-6, -1)
w4 = Window.partitionBy("country", "city").orderBy(F.col("city_length").desc_nulls_first(), "id")
w5 = Window.partitionBy("country").orderBy("product_price_cume_dist", "id").rowsBetween(-2, -1)
w6 = Window.partitionBy("city")

cleaned = (
    df_suppliers
    .filter(F.col("rating") <= 2.5)
    .select(
        "id",
        "supplier_name",
        "country",
        "city",
        "rating",
        F.length("city").alias("city_length"),
        F.coalesce("city", F.lit("N/A")).alias("city_filled"),
        F.coalesce("city", F.lit("N/A")).alias("city_clean"),
    )
)

eligible = (
    cleaned
    .alias("s")
    .join(df_products.alias("ch"), (F.col("ch.supplier_id") == F.col("s.id")) & (~F.col("ch.category").isin(["Home", "Fashion", "Sports"])), "left_anti")
)

with_last = (
    eligible
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.price").alias("product_price"))
)

with_windows = (
    with_last
    .select(
        "*",
        F.cume_dist().over(w2).alias("product_price_cume_dist"),
        F.avg("city_length").over(w3).alias("city_length_avg_6_prior"),
    )
)

ranked = (
    with_windows
    .withColumn("row_num", F.row_number().over(w4))
    .filter(F.col("row_num") == 1)
)

enriched = (
    ranked
    .select(
        "id",
        "supplier_name",
        "country",
        "city",
        "city_length",
        "city_clean",
        "product_price",
        "product_price_cume_dist",
        "city_length_avg_6_prior",
        "row_num",
        F.avg("product_price_cume_dist").over(w5).alias("product_price_cume_dist_avg_2_prior"),
        F.round((F.col("rating") / F.sum("rating").over(w6)), 4).alias("rating_share"),
    )
)

valid = (
    enriched
    .filter(F.col("city_length").between(35, 52))
    .select(
        "id",
        "country",
        "city",
        "city_length",
        "city_clean",
        "product_price",
        "product_price_cume_dist",
        "city_length_avg_6_prior",
        "row_num",
        "product_price_cume_dist_avg_2_prior",
        "rating_share",
        F.coalesce("product_price_cume_dist", F.lit(0)).alias("product_price_cume_dist_filled"),
    )
)

result = (
    valid
    .select(
        "id",
        "city",
        "city_clean",
        "product_price",
        "product_price_cume_dist",
        "city_length_avg_6_prior",
        "product_price_cume_dist_avg_2_prior",
        "rating_share",
        "product_price_cume_dist_filled",
        F.when(F.col("city").rlike("^(Singapore|Tokyo)$"), "GROUP_A").otherwise("EXTENDED").alias("city_zone"),
    )
)
