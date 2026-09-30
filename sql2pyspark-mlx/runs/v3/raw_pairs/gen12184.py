from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("supplier_id").isNotNull())
    .select("id", "supplier_id")
    .intersect(
        df_products
        .filter(F.col("brand").isNotNull())
        .select("id", "supplier_id")
    )
    .orderBy("supplier_id")
)
