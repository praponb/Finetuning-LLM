from pyspark.sql import functions as F

result = (
    df_products
    .groupBy("supplier_id")
    .agg(
        (F.max("cost") - F.min("cost")).alias("cost_range"),
        F.sum(F.when(F.col("category") == "Grocery", F.col("price")).otherwise(0)).alias("grocery_price"),
    )
)
