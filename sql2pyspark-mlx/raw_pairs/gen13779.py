from pyspark.sql import functions as F

filtered = (
    df_products
    .filter((F.col("launch_date") >= F.to_date(F.lit("2023-07-18"))) & (F.col("brand").eqNullSafe(F.lit("Initech"))))
    .select(
        "*",
        F.round((F.col("price") / F.col("cost")), 2).alias("margin"),
        F.coalesce("brand", F.lit("OTHER")).alias("brand_filled"),
    )
)

result = (
    filtered
    .groupBy("supplier_id", "brand")
    .pivot("category", ["Grocery", "Electronics", "Fashion"])
    .agg(F.max("price"))
)
