from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("price").between(440, 620) | F.col("brand").isin(["Initech", "Umbrella", "Globex"]))
    .select("supplier_id")
    .orderBy(F.col("supplier_id").desc())
)
