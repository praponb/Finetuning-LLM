from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("price") > 700)
    .select("launch_date", "category")
    .unionByName(
        df_products
        .filter(F.col("cost") >= 640)
        .select("launch_date", "category")
    )
    .orderBy(F.col("category").desc())
)
