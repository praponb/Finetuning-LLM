from pyspark.sql import functions as F

sku_rows = (
    df_skus
    .filter(F.upper(F.trim(F.col("sku_code"))).like("E%"))
    .select(F.col("sku_code").alias("name"), "category")
)

sku_rows_2 = (
    df_skus
    .filter((F.col("is_active")) & (F.col("category") == "Toys"))
    .select(F.col("sku_code").alias("name"), "category")
)

result = sku_rows.unionByName(sku_rows_2)
