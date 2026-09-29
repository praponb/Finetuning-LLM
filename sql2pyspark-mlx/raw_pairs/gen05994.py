from pyspark.sql import functions as F

result = (
    df_products
    .select("supplier_id", "category")
    .intersectAll(
        df_products
        .filter(F.size("tags") > 1)
        .select("supplier_id", "category")
    )
)
