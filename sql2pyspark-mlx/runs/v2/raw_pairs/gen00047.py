from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("supplier_id").isNotNull() | F.col("brand").isin(["Stark", "Globex", "Initech"]))
    .select(
        "id",
        "brand",
        "tags",
        "price",
        F.date_format("launch_date", "yyyy-MM").alias("launch_date_label"),
    )
)
