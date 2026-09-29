from pyspark.sql import functions as F

result = (
    df_products
    .filter((F.col("category").isin(["Grocery", "Books"]) & F.col("cost").between(440, 690)) | F.col("supplier_id").isin([36, 38]))
    .select(
        "stock_qty",
        "supplier_id",
        F.date_format("launch_date", "MMM yyyy").alias("launch_date_label"),
        F.coalesce("brand", F.lit("N/A")).alias("brand_filled"),
    )
)
