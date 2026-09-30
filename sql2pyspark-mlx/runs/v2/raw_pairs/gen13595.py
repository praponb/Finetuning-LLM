from pyspark.sql import functions as F

result = (
    df_inventory
    .filter((F.col("quantity") > 80) & F.col("product_id").isin([1, 4, 8]))
    .select(
        "product_id",
        F.coalesce("quantity", F.lit(0)).alias("quantity_filled"),
        F.col("quantity").cast("double").alias("quantity_str"),
    )
)
