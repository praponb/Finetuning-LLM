from pyspark.sql import functions as F

result = (
    df_products
    .filter((F.col("price") <= 1060) & F.col("brand").isin(["Globex", "Stark", "Acme"]))
    .groupBy("category")
    .agg(
        (F.max("stock_qty") - F.min("stock_qty")).alias("stock_qty_range"),
        F.max("cost").alias("max_cost"),
    )
    .filter(F.col("stock_qty_range") < 43)
    .orderBy(F.col("category").desc(), "max_cost")
)
