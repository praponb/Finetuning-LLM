from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.array_contains("tags", "new"))
    .select("price")
    .subtract(df_shipments.select("cost"))
    .orderBy("price")
)
