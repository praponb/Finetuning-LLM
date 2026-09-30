from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("brand").isNotNull())
    .select("id", "brand")
    .intersect(
        df_products
        .filter(F.length("product_name") >= 11)
        .select("id", "brand")
    )
)
