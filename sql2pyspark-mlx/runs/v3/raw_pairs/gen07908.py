from pyspark.sql import functions as F

result = (
    df_products
    .filter((F.col("category") == "Fashion") & F.col("price").isNull())
    .select("product_name")
    .orderBy("product_name")
)
