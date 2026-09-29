from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.array_contains("tags", "eco"))
    .select(
        "id",
        "price",
        F.add_months("launch_date", 6).alias("launch_date_plus_months"),
    )
)
