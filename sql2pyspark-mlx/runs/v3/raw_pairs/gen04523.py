from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("stock_qty") < 102)
    .select("brand", "product_name")
    .subtract(
        df_products
        .filter(~F.col("category").isin(["Books", "Fashion"]))
        .select("brand", "product_name")
    )
)
