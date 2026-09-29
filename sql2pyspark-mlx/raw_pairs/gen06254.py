from pyspark.sql import functions as F

result = (
    df_products
    .select("id")
    .intersect(
        df_products
        .filter(F.lower(F.trim("product_name")) == "backpack xl")
        .select("id")
    )
)
