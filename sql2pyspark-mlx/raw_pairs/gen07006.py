from pyspark.sql import functions as F

result = (
    df_products
    .select("supplier_id")
    .subtract(
        df_products
        .filter(F.length("product_name") > 6)
        .select("supplier_id")
    )
)
