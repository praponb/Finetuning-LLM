from pyspark.sql import functions as F

sku_side = df_skus.select(F.lit("sku").alias("origin"), "id", F.col("weight_kg").alias("value"))

sku_side_2 = (
    df_skus
    .filter((F.col("weight_kg") >= 6.5) & (F.lower(F.col("sku_code")).like("%l-%")))
    .select(F.lit("sku_2").alias("origin"), "id", F.col("weight_kg").alias("value"))
)

sku_rows = (
    df_skus
    .filter((F.upper(F.trim(F.col("sku_code"))).like("E%")) & (F.col("category") == "Apparel"))
    .select(F.lit("sku_3").alias("origin"), "id", F.col("unit_cost").alias("value"))
)

result = (
    sku_side
    .unionByName(sku_side_2)
    .unionByName(sku_rows)
    .orderBy("id", "value")
)
