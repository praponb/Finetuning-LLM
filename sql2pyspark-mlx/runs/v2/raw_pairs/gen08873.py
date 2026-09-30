from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("brand") == "Initech")
    .select(
        "id",
        "brand",
        "tags",
        F.date_format("launch_date", "yyyy-MM-dd").alias("launch_date_label"),
    )
)
