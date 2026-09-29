from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.stock_qty").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("country", "product_brand").orderBy("rating_whole", "id")
w3 = Window.partitionBy("country").orderBy(F.col("prev_rating_whole").asc_nulls_last(), "id")

valid = (
    df_suppliers
    .filter(F.col("city").isNotNull())
    .withColumn("rating_whole", F.floor("rating"))
    .withColumn("city_filled", F.coalesce("city", F.lit("N/A")))
)

not_flagged = (
    valid
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")) & (F.col("c.supplier_id").isNotNull()), "left_semi")
)

with_last = (
    not_flagged
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.country",
        "s.rating_whole",
        F.col("c.category").alias("product_category"),
        F.col("c.brand").alias("product_brand"),
    )
)

metrics = (
    with_last
    .select(
        "id",
        "country",
        F.lag("rating_whole").over(w2).alias("prev_rating_whole"),
        F.row_number().over(w2).alias("row_seq"),
    )
)

ranked = (
    metrics
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
    .select("id", "country", "prev_rating_whole")
)

result = (
    ranked
    .select(
        "id",
        "country",
        "prev_rating_whole",
        F.when(F.col("country").rlike("^(US|UK)$"), "OTHER_ZONE").when(F.col("country").rlike("^(TH|SG)$"), "PRIORITY").otherwise("CORE").alias("country_zone"),
    )
    .orderBy(F.col("id").desc_nulls_first())
)
