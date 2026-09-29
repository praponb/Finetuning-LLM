from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("price") > 630)
    .select("id", "tags", "category")
    .orderBy("category")
)
