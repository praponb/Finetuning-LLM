from pyspark.sql import functions as F

cleaned = (
    df_suppliers
    .filter(~F.col("country").isin(["UK", "TH"]))
    .select(
        "*",
        F.round("rating", 0).alias("rating_rounded"),
        F.coalesce("city", F.lit("NONE")).alias("city_filled"),
    )
)

result = (
    cleaned
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")) & (F.col("c.launch_date") > "2025-02-11"), "left_semi")
)
