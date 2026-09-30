from pyspark.sql import functions as F

base = (
    df_products
    .filter(~F.col("brand").eqNullSafe(F.lit("Stark")))
    .select(
        "*",
        F.array_join("tags", ", ").alias("tags_text"),
        F.coalesce("brand", F.lit("N/A")).alias("brand_clean"),
    )
)

result = (
    base
    .alias("s")
    .join(df_store_sales.alias("x"), (F.col("x.product_id") == F.col("s.id")) & (F.col("x.sale_date") <= F.col("s.launch_date")), "left_semi")
)
