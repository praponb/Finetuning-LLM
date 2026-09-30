from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("carrier") == "Kerry")
    .select("cost")
    .union(df_products.select("price"))
    .orderBy(F.col("cost").desc())
)
