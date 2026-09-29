from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("supplier_id") == 33)
    .select("launch_date")
    .unionByName(df_products.filter(F.col("price") < 690).select("launch_date"))
    .distinct()
    .orderBy("launch_date")
)
