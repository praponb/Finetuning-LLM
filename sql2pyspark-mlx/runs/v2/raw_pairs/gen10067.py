from pyspark.sql import functions as F

result = (
    df_products
    .groupBy("supplier_id")
    .pivot("brand", ["Stark", "Initech", "Umbrella"])
    .agg(F.count("cost"))
)
