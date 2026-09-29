from pyspark.sql import functions as F

result = (
    df_products
    .filter(~F.col("category").isin(["Sports", "Grocery", "Books"]))
    .select("id", "product_name")
)
