from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("brand").isin(["Acme", "Initech"]))
    .withColumn("greatest_cost_stock_qty", F.greatest("cost", "stock_qty"))
    .withColumn("category_group", F.when(F.col("category").isin(["Books", "Sports"]), "Core").otherwise("Other"))
)
