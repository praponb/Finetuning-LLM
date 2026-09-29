from pyspark.sql import functions as F

result = (
    df_products
    .filter((F.col("category").isin(["Fashion", "Home", "Electronics"]) & F.col("cost").between(400, 600)) | (F.col("launch_date") > "2023-04-07"))
    .select("id", "brand", "supplier_id")
)
