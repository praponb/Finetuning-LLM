from pyspark.sql import functions as F

result = (
    df_inventory
    .select(
        "id",
        "product_id",
        "quantity",
        F.when(F.col("quantity") >= 234, "High").when(F.col("quantity") >= 82, "Medium").otherwise("Low").alias("quantity_tier"),
    )
    .orderBy(F.col("id").desc(), "quantity")
    .limit(1)
)
