from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("product_name").like("Bac%"))
    .select("id", "brand", "supplier_id", "price")
    .orderBy("supplier_id", "id")
    .limit(10)
)
