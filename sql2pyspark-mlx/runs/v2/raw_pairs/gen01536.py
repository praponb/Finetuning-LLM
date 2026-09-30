from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("brand").isin(["Umbrella", "Acme", "Initech"]) & ((F.col("launch_date") < "2024-03-01") | (F.col("brand") == "Acme")))
    .select("tags", "launch_date", "price")
)
