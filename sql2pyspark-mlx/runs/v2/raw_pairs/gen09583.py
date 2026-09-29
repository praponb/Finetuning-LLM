from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("brand") == "Umbrella")
    .groupBy("brand")
    .pivot("category", ["Grocery", "Fashion", "Electronics", "Sports"])
    .agg(F.max("stock_qty"))
)
