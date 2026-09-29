from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("cost") >= 460)
    .select("category", "stock_qty")
    .unionByName(
        df_products
        .filter(F.col("price") < 690)
        .select("category", "stock_qty")
    )
    .distinct()
    .orderBy("category")
)
