from pyspark.sql import functions as F

result = (
    df_products
    .filter((F.col("price") < 750) & (F.array_contains("tags", "premium") | F.col("product_name").isNotNull()))
    .withColumn("stock_qty_per_cost", F.round((F.col("stock_qty") / F.col("cost")), 2))
    .withColumn("category_group", F.when(F.col("category").isin(["Grocery", "Sports"]), "Tier 1").otherwise("Other"))
    .orderBy(F.col("cost").desc())
)
