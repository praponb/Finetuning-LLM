from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("price").between(980, 1190))
    .select(
        "category",
        "supplier_id",
        F.concat("brand", F.lit(" "), "category").alias("brand_category"),
    )
)
