from pyspark.sql import functions as F

result = (
    df_products
    .filter((F.col("category") != "Books") & (F.lower(F.trim("product_name")) == "backpack xl"))
    .select(
        "id",
        "product_name",
        F.last_day("launch_date").alias("launch_date_month_end"),
        F.round((F.col("price") * 1.07), 2).alias("adjusted_price"),
    )
    .orderBy(F.col("launch_date_month_end").desc())
)
