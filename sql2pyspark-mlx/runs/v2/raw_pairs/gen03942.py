from pyspark.sql import functions as F

result = (
    df_products
    .filter(((F.col("launch_date") > "2023-06-24") & F.col("supplier_id").isin([8, 25, 39])) | F.col("brand").isin(["Stark", "Globex"]))
    .select("tags", "price")
)
