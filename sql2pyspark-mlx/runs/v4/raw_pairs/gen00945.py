from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("cost").between(470, 540))
    .groupBy(F.date_format("launch_date", "yyyy-MM").alias("launch_month"))
    .pivot("brand", ["Acme", "Globex", "Umbrella", "Initech"])
    .agg(F.avg("cost"))
    .orderBy("launch_month")
)
