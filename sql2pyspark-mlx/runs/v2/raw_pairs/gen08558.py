from pyspark.sql import functions as F

result = (
    df_products
    .filter((F.col("cost") > 260) & (F.col("cost") > 220))
    .select(
        "id",
        "brand",
        "stock_qty",
        F.length("brand").alias("brand_length"),
        F.add_months("launch_date", 6).alias("launch_date_plus_months"),
    )
)
