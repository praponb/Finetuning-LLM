from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.partitionBy("brand", "category")
w2 = Window.partitionBy(F.date_trunc("YEAR", "launch_date"), "supplier_id")
w3 = Window.partitionBy(F.date_trunc("YEAR", "launch_date"))
w4 = Window.partitionBy(F.year("launch_date"), "brand")

result = (
    df_products
    .select(
        "*",
        F.min("price").over(w_running).alias("brand_min_price"),
        F.sum("price").over(w2).alias("launch_date_period_sum_price"),
        F.round((F.col("price") / F.sum("price").over(w3)), 4).alias("price_share"),
        F.avg("price").over(w4).alias("launch_date_year_avg_price"),
    )
)
