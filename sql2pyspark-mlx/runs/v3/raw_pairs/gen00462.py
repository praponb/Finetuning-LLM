from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("stock_qty") != 125)
    .select("launch_date")
    .intersect(df_stores.select("opened_date"))
)
