from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.array_contains("tags", "clearance") & F.col("brand").isin(["Acme", "Globex"]))
    .select("id", "tags")
)
