from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("category") == "Home")
    .select("supplier_id", F.round((F.col("cost") * 1.1), 2).alias("adjusted_cost"))
    .orderBy("supplier_id")
)
