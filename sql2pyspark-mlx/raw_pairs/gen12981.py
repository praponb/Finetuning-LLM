from pyspark.sql import functions as F

result = (
    df_products
    .filter(~F.col("brand").isin(["Umbrella", "Acme", "Stark"]))
    .select("id", "product_name")
    .intersect(df_products.filter(F.size("tags") >= 1).select("id", "product_name"))
)
