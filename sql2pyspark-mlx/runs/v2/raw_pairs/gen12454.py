from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.size("tags") > 1)
    .select("category", "product_name")
    .subtract(
        df_products
        .filter(F.col("brand").isNotNull())
        .select("category", "product_name")
    )
)
