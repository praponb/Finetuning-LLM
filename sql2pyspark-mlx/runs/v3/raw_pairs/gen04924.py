from pyspark.sql import functions as F

result = (
    df_products
    .filter((F.col("launch_date") >= "2025-07-01") & F.col("category").isin(["Home", "Sports", "Books"]))
    .select(
        "id",
        "category",
        "supplier_id",
        "launch_date",
        F.add_months("launch_date", 6).alias("launch_date_plus_months"),
    )
)
