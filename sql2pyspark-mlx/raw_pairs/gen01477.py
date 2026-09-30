from pyspark.sql import functions as F

result = (
    df_products
    .groupBy(F.date_format("launch_date", "yyyy-MM").alias("launch_month"))
    .pivot("brand", ["Stark", "Acme", "Initech", "Umbrella"])
    .agg(F.count("cost"))
)
