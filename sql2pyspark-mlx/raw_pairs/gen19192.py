from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy(F.col("rating").desc(), "id")

filtered = (
    df_suppliers
    .filter(F.col("rating") > 3.0)
    .select(
        "id",
        "supplier_name",
        "country",
        "city",
        "rating",
        F.coalesce("rating", F.lit(0)).alias("rating_filled"),
    )
)

base = (
    filtered
    .filter(F.col("city").isNotNull())
    .select(
        "id",
        "supplier_name",
        "city",
        "rating",
        "rating_filled",
        F.concat_ws("-", "city", "country").alias("city_country_key"),
        F.coalesce("rating_filled", F.lit(0)).alias("rating_filled_filled"),
    )
)

latest = (
    base
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
)

base_suppliers = (
    latest
    .filter(F.col("city").isNotNull())
    .select(
        "id",
        "city",
        "rating",
        "rating_filled",
        "rn",
        F.length("supplier_name").alias("supplier_name_length"),
        F.when(F.col("city").isin(["Bangkok", "London"]), "Group A").otherwise("Other").alias("city_group"),
    )
)

eligible = (
    base_suppliers
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")) & (F.length(F.col("c.product_name")) > 13), "left_anti")
)

result = (
    eligible
    .groupBy(
        F.when(F.col("rating") >= 3.4, "PREMIUM").otherwise("STANDARD").alias("rating_tier"),
    )
    .agg(
        F.round(F.sum("rating") / F.nullif(F.count(F.when(F.col("city") == "Tokyo", 1)), F.lit(0)), 2).alias("rating_per_tokyo"),
        F.percentile_approx("rating", 0.95).alias("rating_p95"),
    )
    .filter(F.col("rating_p95") > 0)
)
