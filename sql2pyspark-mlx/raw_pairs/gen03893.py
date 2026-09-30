from pyspark.sql import functions as F

filtered = (
    df_products
    .filter(F.col("launch_date") >= F.lit("2023-08-22").cast("date"))
    .withColumn("cost_level", F.when(F.col("cost") >= 650, "High").when(F.col("cost") >= 280, "Medium").otherwise("Low"))
)

kept_keys = (
    filtered
    .select("id", "supplier_id")
    .subtract(
        df_products
        .filter(F.col("launch_date") < "2024-03-01")
        .select("id", "supplier_id")
    )
)

reconciled = (
    filtered
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.supplier_id") == F.col("k.supplier_id")), "inner")
    .select("s.*")
)

valid = (
    reconciled
    .filter(F.col("brand").isNotNull() & (~F.col("brand").eqNullSafe(F.lit("Stark"))))
    .select(
        "product_name",
        "category",
        "stock_qty",
        "launch_date",
        "supplier_id",
        F.round("price", 1).alias("price_rounded"),
    )
)

enriched = (
    valid
    .alias("e")
    .join(df_suppliers.alias("d"), F.col("e.supplier_id") == F.col("d.id"), "left")
    .select("e.*", F.col("d.city"), F.col("d.country"), F.col("d.rating"))
)

with_dims = (
    enriched
    .alias("e")
    .join(df_suppliers.alias("s"), F.col("e.supplier_id") == F.col("s.id"), "left")
    .select(
        "e.*",
        F.col("s.country").alias("supplier_country"),
        F.col("s.rating").alias("supplier_rating"),
    )
)

pivoted = (
    with_dims
    .groupBy("supplier_country")
    .pivot("category", ["Electronics", "Sports", "Home"])
    .agg(F.max("stock_qty"))
)

result = pivoted.select("supplier_country", "Sports", "Home")
