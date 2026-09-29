from pyspark.sql import functions as F

result = (
    df_order_items
    .select(
        "id",
        "unit_price",
        F.round((F.col("unit_price") * 1.15), 2).alias("unit_price_scaled"),
    )
    .orderBy(F.col("id").desc())
    .limit(20)
)
