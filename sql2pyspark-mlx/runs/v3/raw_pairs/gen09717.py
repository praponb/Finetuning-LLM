from pyspark.sql import functions as F

result = (
    df_products
    .filter((F.col("supplier_id").isNull() & F.col("supplier_id").isin([4, 18, 32])) | F.col("product_name").like("U%"))
    .select(
        "product_name",
        "launch_date",
        "tags",
        F.initcap(F.trim("brand")).alias("brand_title"),
    )
)
