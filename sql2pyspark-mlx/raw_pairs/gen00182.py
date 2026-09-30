from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy(F.col("rating").desc_nulls_first(), "id")
w2 = Window.partitionBy("city").orderBy("rn", "id")
w3 = Window.partitionBy("country")

latest = (
    df_suppliers
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
)

enriched = (
    latest
    .filter(F.col("rating") <= 3.5)
    .select(
        "id",
        "country",
        "city",
        "rating",
        "rn",
        F.lag("rating", 1, 0).over(w2).alias("prev_rating"),
        F.round((F.col("rating") / F.sum("rating").over(w3)), 4).alias("rating_share"),
    )
)

matched_keys = (
    enriched
    .select("country")
    .subtract(
        df_suppliers
        .filter(F.col("country") == "US")
        .select("country")
    )
)

verified = (
    enriched
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.country") == F.col("k.country")), "inner")
    .select("s.*")
)

eligible = (
    verified
    .alias("s")
    .join(df_products.alias("x"), (F.col("x.supplier_id") == F.col("s.id")) & (F.col("x.category").isin(["Fashion", "Grocery", "Books"])), "left_semi")
)

valid = (
    eligible
    .filter(F.col("city").isNotNull() & F.col("rating").between(1.9, 2.2))
    .select(
        "*",
        F.col("rating_share").cast("int").alias("rating_share_int"),
        F.least("prev_rating", "rn").alias("least_prev_rating_rn"),
        F.coalesce("city", F.lit("N/A")).alias("city_clean"),
    )
)

result = (
    valid
    .select(
        "id",
        "country",
        "city",
        "rating",
        "rn",
        "prev_rating",
        "rating_share",
        "rating_share_int",
        "least_prev_rating_rn",
        F.when(F.col("city").rlike("^(Tokyo|Chiang Mai)$"), "PRIORITY").when(F.col("city").rlike("^(Bangkok|New York)$"), "EXTENDED").otherwise("OTHER_ZONE").alias("city_zone"),
    )
)
