from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("launch_date").between("2025-04-04", "2025-05-01"))
    .groupBy("category")
    .pivot("brand", ["Initech", "Stark", "Acme", "Umbrella"])
    .agg(F.sum("price"))
    .orderBy("category")
)
