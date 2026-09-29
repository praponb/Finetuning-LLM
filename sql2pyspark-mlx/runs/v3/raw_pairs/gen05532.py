from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("supplier_id").isin([7, 11, 33]) & (F.col("brand").isin(["Initech", "Umbrella"]) | F.col("brand").isin(["Umbrella", "Stark", "Initech"])))
    .select(
        "id",
        "category",
        "launch_date",
        "cost",
        F.array_join("tags", ", ").alias("tags_text"),
        F.coalesce("brand", F.lit("N/A")).alias("brand_filled"),
    )
)
