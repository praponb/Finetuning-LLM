from pyspark.sql import functions as F

valid = (
    df_skus
    .filter((F.col("category").isin("Food", "Electronics")) & (F.col("weight_kg").between(6.5, 18.5)))
    .select(
        "id",
        "sku_code",
        "category",
        "unit_cost",
        "weight_kg",
        "labels",
    )
)

sku_rows = (
    valid
    .filter((F.upper(F.trim(F.col("sku_code"))).like("E%")) & (F.size(F.col("labels")) >= 1))
    .select(
        F.col("category").alias("name"),
        "id",
        "weight_kg",
        F.lit("sku").alias("record_type"),
    )
)

sku_side = (
    df_skus
    .select(
        "sku_code",
        "id",
        "weight_kg",
        F.lit("sku_2").alias("record_type"),
    )
)

result = sku_rows.union(sku_side)
