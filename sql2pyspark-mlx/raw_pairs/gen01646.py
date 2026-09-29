from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.launch_date").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("product_category").orderBy(F.col("rating").desc(), "id")
w3 = Window.orderBy("rating", "id").rowsBetween(-5, -1)
w4 = Window.partitionBy("brand").orderBy("rating", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_suppliers
    .filter(~F.col("city").eqNullSafe(F.lit("Chiang Mai")))
    .select(
        "id",
        "country",
        "city",
        "rating",
        F.trim("country").alias("clean_country"),
        F.lower("country").alias("country_lower"),
        F.coalesce("city", F.lit("UNKNOWN")).alias("city_clean"),
    )
)

eligible = (
    filtered
    .alias("s")
    .join(df_products.alias("x"), (F.col("x.supplier_id") == F.col("s.id")) & (F.col("x.stock_qty") != 261), "left_anti")
)

with_latest = (
    eligible
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.category").alias("product_category"), F.col("c.brand"))
)

enriched = (
    with_latest
    .filter(F.col("product_category") == "Electronics")
    .select(
        "id",
        "country",
        "city",
        "rating",
        F.ntile(3).over(w2).alias("rating_bucket"),
        F.avg("rating").over(w3).alias("rating_avg_5_prior"),
        F.first("product_category", ignorenulls=True).over(w4).alias("first_known_product_category"),
    )
)

matched_keys = (
    enriched
    .select("country", "city")
    .subtract(
        df_suppliers
        .filter(F.col("city").isin(["Singapore", "Bangkok", "Tokyo"]))
        .select("country", "city")
    )
)

verified = (
    enriched
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.country") == F.col("k.country")) & (F.col("s.city") == F.col("k.city")), "inner")
    .select("s.*")
)

result = (
    verified
    .select(
        "id",
        "country",
        "city",
        "rating",
        "rating_bucket",
        "rating_avg_5_prior",
        "first_known_product_category",
        F.when(F.col("country").rlike("^(US|JP)$"), "CORE").otherwise("OTHER_ZONE").alias("country_zone"),
    )
    .orderBy("country_zone")
)
