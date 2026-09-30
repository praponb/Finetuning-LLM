from pyspark.sql import functions as F

result = (
    df_customers
    .select("city")
    .intersect(
        df_products
        .filter(F.upper(F.trim("product_name")) == "BACKPACK XL")
        .select("brand")
    )
)
