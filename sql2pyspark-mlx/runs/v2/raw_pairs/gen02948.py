from pyspark.sql import functions as F

result = (
    df_products
    .filter(~F.col("category").isin(["Fashion", "Sports"]))
    .select("id", "brand")
    .unionByName(
        df_products
        .filter(F.col("launch_date") < "2024-01-01")
        .select("id", "brand")
    )
    .orderBy("brand")
)
