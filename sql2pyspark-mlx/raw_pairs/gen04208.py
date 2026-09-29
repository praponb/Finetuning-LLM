from pyspark.sql import functions as F

filtered = (
    df_products
    .filter((~F.col("brand").eqNullSafe(F.lit("Stark"))) & (F.col("price") < 360))
    .withColumn("category_prefix", F.substring("category", 1, 4))
    .withColumn("brand_filled", F.coalesce("brand", F.lit("NONE")))
)

result = (
    filtered
    .select("id", "brand", "price", F.col("cost").alias("COST_VAL"))
    .unpivot(
        ids=["id", "brand"],
        values=["price", "COST_VAL"],
        variableColumnName="metric",
        valueColumnName="val",
    )
)
