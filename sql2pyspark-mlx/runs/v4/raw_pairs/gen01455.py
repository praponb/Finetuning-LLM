from pyspark.sql import functions as F

result = (
    df_orders
    .filter(~F.col("channel").isin(["partner", "mobile", "web"]))
    .select("shipping_fee")
    .union(df_products.filter(F.col("price") < 1190).select("cost"))
    .distinct()
)
