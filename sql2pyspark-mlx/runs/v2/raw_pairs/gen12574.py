from pyspark.sql import functions as F

result = (
    df_products
    .groupBy("supplier_id")
    .agg(
        F.max("cost").alias("max_cost"),
        F.max("price").alias("max_price"),
    )
)
