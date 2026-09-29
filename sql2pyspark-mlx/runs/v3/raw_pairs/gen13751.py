from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("cost") >= 93)
    .select("cost")
    .intersect(df_products.select("price"))
)
