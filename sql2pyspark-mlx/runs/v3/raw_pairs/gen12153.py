from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("product_name").like("%Runn%"))
    .select(
        "price",
        "brand",
        "supplier_id",
        F.quarter("launch_date").alias("launch_date_quarter"),
    )
)
