from pyspark.sql import functions as F

result = (
    df_products
    .groupBy("category", "supplier_id")
    .pivot("brand", ["Initech", "Acme"])
    .agg(F.sum("price").alias("amount"), F.count(F.lit(1)).alias("cnt"))
)
