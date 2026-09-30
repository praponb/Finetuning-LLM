from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("brand") == "Acme")
    .select(
        "id",
        "tags",
        "stock_qty",
        F.when(F.col("cost") >= 670, "High").when(F.col("cost") >= 230, "Medium").otherwise("Low").alias("cost_tier"),
        F.length("category").alias("category_length"),
    )
    .orderBy(F.col("stock_qty").desc(), "cost_tier")
)
