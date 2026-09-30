from pyspark.sql import functions as F

filtered = (
    df_products
    .filter(F.col("launch_date") >= F.lit("2024-04-11").cast("date"))
    .select(
        "*",
        F.date_add("launch_date", 14).alias("launch_date_plus_14d"),
        F.coalesce("brand", F.lit("UNKNOWN")).alias("resolved_brand"),
    )
)

result = (
    filtered
    .alias("s")
    .join(df_store_sales.alias("c"), (F.col("c.product_id") == F.col("s.id")) & (F.col("c.sale_date") <= "2023-04-08"), "left_semi")
)
