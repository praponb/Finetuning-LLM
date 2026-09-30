from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("brand").isin(["Stark", "Acme"]))
    .orderBy("launch_date")
)
