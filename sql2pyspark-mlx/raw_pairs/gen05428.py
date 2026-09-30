from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("cost") <= 390)
    .select("id", "product_name", "category")
    .unionByName(
        df_products
        .filter(F.col("brand").isNotNull())
        .select("id", "product_name", "category")
    )
    .distinct()
)
