from pyspark.sql import functions as F

result = (
    df_products
    .groupBy("category", "brand")
    .agg(
        F.countDistinct("price").alias("unique_suppliers"),
        F.sum(F.when(F.col("category") == "Grocery", 1).otherwise(0)).alias("grocery_count"),
    )
    .filter(F.col("unique_suppliers") < 9)
    .select("category", "brand", "unique_suppliers", "grocery_count")
    .orderBy("grocery_count", "category", "brand")
    .limit(3)
)
