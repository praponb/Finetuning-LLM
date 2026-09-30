from pyspark.sql import functions as F

result = (
    df_products
    .filter(~F.col("brand").isin(["Acme", "Globex"]))
    .select("id", "category")
    .intersect(df_products.filter(F.col("stock_qty") < 158).select("id", "category"))
)
