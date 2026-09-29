from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.price").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("city").orderBy("price", "id").rowsBetween(-3, Window.currentRow)
w3 = Window.partitionBy("city").orderBy(F.col("last4_sum_rating").asc_nulls_first(), "id")

cleaned = (
    df_suppliers
    .filter(F.col("supplier_name").like("Tok%"))
    .select(
        "*",
        F.when(F.col("country").isin(["JP", "TH"]), "Core").otherwise("Other").alias("country_group"),
        F.coalesce("city", F.lit("N/A")).alias("city_filled"),
    )
)

with_latest = (
    cleaned
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.price"))
)

with_windows = (
    with_latest
    .select(
        "id",
        "country",
        "city",
        "rating",
        "price",
        F.count("rating").over(w2).alias("last4_count_rating"),
        F.max("rating").over(w2).alias("last4_max_rating"),
        F.sum("rating").over(w2).alias("last4_sum_rating"),
    )
)

ranked = (
    with_windows
    .withColumn("dedup_rank", F.row_number().over(w3))
    .filter(F.col("dedup_rank") == 1)
)

kept = (
    ranked
    .alias("s")
    .join(df_products.alias("ch"), (F.col("ch.supplier_id") == F.col("s.id")) & (F.col("ch.supplier_id") == 8), "left_anti")
)

result = (
    kept
    .select(
        "id",
        "country",
        "city",
        "last4_count_rating",
        "dedup_rank",
        F.when(F.col("country").rlike("^(US|TH)$"), "OTHER_ZONE").otherwise("PRIORITY").alias("country_zone"),
    )
)
