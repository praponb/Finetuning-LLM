from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("price").between(680, 750))
    .select(
        "id",
        "supplier_id",
        "tags",
        "product_name",
        F.round("cost", 1).alias("cost_rounded"),
    )
    .orderBy("product_name")
)
