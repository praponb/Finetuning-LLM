from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.month("launch_date").isin([7, 8, 12]) & F.col("category").isin(["Grocery", "Sports"]) & F.col("supplier_id").isin([18, 30]))
    .select(
        "id",
        "brand",
        "product_name",
        F.upper("product_name").alias("product_name_upper"),
        F.concat("product_name", F.lit("/"), "category").alias("product_name_category"),
    )
)
