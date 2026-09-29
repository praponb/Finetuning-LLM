from pyspark.sql import functions as F

result = (
    df_products
    .groupBy(F.date_trunc("MONTH", "launch_date").alias("launch_month"))
    .pivot("brand", ["Globex", "Stark", "Initech", "Umbrella"])
    .agg(F.sum("stock_qty"))
    .orderBy("launch_month")
)
