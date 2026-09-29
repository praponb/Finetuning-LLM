from pyspark.sql import functions as F

result = (
    df_products
    .groupBy("supplier_id")
    .pivot("category", ["Grocery", "Sports", "Home"])
    .agg(F.max("price"))
    .orderBy("supplier_id")
)
