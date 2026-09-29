from pyspark.sql import functions as F

result = (
    df_products
    .select("id", "supplier_id", "brand")
    .orderBy("brand", "id")
    .limit(5)
)
