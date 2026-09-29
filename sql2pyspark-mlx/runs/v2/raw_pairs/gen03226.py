from pyspark.sql import functions as F

result = (
    df_products
    .agg(
        F.max("price").alias("max_price"),
        F.countDistinct("cost").alias("unique_suppliers"),
    )
    .orderBy("unique_suppliers")
)
