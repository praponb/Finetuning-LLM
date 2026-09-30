from pyspark.sql import functions as F

sku_rows = (
    df_skus
    .filter(F.col("category") != "Toys")
    .select(F.col("id").alias("qty"))
)

sku_rows_2 = df_skus.select("id")

result = (
    sku_rows
    .exceptAll(sku_rows_2)
    .orderBy("qty")
)
