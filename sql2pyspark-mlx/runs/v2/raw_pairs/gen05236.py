from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("product_name").like("Bac%"))
    .groupBy("category")
    .pivot("brand", ["Stark", "Acme"])
    .agg(F.count("price"))
    .orderBy("category")
)
