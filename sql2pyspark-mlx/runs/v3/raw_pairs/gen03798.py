from pyspark.sql import functions as F

result = (
    df_products
    .filter((F.col("brand") == "Acme") & (F.col("launch_date") <= "2023-04-17"))
    .orderBy("id", F.col("price").desc())
)
