from pyspark.sql import functions as F

result = (
    df_products
    .select(
        "stock_qty",
        F.add_months("launch_date", 3).alias("launch_date_plus_months"),
        F.array_join("tags", ", ").alias("tags_text"),
    )
)
