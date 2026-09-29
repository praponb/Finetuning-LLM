from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("supplier_id").isin([16, 36]))
    .select("id", "brand", "stock_qty")
    .unionByName(
        df_products
        .filter(~F.col("brand").isin(["Umbrella", "Stark", "Initech"]))
        .select("id", "brand", "stock_qty")
    )
    .orderBy(F.col("stock_qty").desc())
)
