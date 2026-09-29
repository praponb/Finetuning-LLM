from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("stock_qty") <= 366)
    .select(
        "id",
        "stock_qty",
        "category",
        F.when(F.col("brand").isin(["Initech", "Globex"]), "Priority").otherwise("Other").alias("brand_group"),
    )
    .orderBy(F.col("brand_group").desc())
)
