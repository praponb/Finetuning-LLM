from pyspark.sql import functions as F

result = (
    df_products
    .filter(~((F.col("cost") < 190) & F.col("product_name").like("Ba%")))
    .select("cost")
    .orderBy("cost")
)
