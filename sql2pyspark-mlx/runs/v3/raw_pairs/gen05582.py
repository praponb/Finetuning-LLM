from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("brand").isin(["Initech", "Stark"]))
    .select(
        "product_name",
        "cost",
        F.coalesce("stock_qty", F.lit(0)).alias("stock_qty_filled"),
    )
    .orderBy(F.col("cost").desc())
)
