from pyspark.sql import functions as F

result = (
    df_products
    .select("price")
    .subtract(df_shipments.filter(F.col("order_id") == 20).select("cost"))
    .orderBy(F.col("price").desc())
)
