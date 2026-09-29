from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("price") < 350)
    .select("product_name")
    .unionByName(
        df_products
        .filter(F.col("supplier_id").isin([21, 31, 34, 35]))
        .select("product_name")
    )
)
