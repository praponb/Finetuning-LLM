from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("cost").between(360, 510))
    .select("brand", "stock_qty")
    .unionByName(
        df_products
        .filter(F.lower("product_name").like("%mar%"))
        .select("brand", "stock_qty")
    )
)
