from pyspark.sql import functions as F

result = (
    df_products
    .filter((F.col("price") > 580) & (F.length("product_name") >= 14))
    .select(
        "id",
        "category",
        "product_name",
        F.when(F.col("price") >= 1260, "High").when(F.col("price") >= 230, "Medium").otherwise("Low").alias("price_tier"),
        F.add_months("launch_date", 12).alias("launch_date_plus_months"),
    )
)
