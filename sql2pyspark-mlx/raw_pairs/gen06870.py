from pyspark.sql import functions as F

result = (
    df_store_sales
    .select(
        "id",
        "product_id",
        F.when(F.col("revenue") >= 4400, "High").when(F.col("revenue") >= 600, "Medium").otherwise("Low").alias("revenue_level"),
    )
)
