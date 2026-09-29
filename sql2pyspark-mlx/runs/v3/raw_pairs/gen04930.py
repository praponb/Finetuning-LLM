from pyspark.sql import functions as F

result = (
    df_store_sales
    .select("revenue")
    .intersect(df_products.filter(F.col("supplier_id").isNotNull()).select("price"))
    .orderBy("revenue")
)
