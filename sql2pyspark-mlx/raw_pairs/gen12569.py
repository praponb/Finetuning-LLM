from pyspark.sql import functions as F

result = (
    df_products
    .filter((F.col("category").isin(["Grocery", "Home"]) & (F.col("price") < 920)) | F.col("brand").isin(["Initech", "Globex"]))
)
