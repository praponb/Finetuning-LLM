from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("stock_qty") <= 267)
    .select("launch_date")
    .unionByName(
        df_products
        .filter(F.col("cost").between(660, 720))
        .select("launch_date")
    )
    .orderBy("launch_date")
)
