from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.array_contains("tags", "premium") & (F.col("supplier_id") == 9) & (F.year("launch_date") == 2023))
    .withColumn("cost_int", F.col("cost").cast("int"))
    .orderBy("cost_int", "stock_qty", "id")
    .limit(1)
)
