from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("supplier_id").isNull())
    .select("id", "product_name")
    .unionByName(
        df_products
        .filter(F.col("brand").isNotNull())
        .select("id", "product_name")
    )
    .orderBy("id")
)
