from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("supplier_id").isNotNull())
    .select("launch_date", "brand")
    .unionByName(df_products.filter(F.col("cost") < 210).select("launch_date", "brand"))
    .distinct()
    .orderBy("launch_date")
)
