from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("stock_qty").between(112, 144))
    .select(
        "launch_date",
        "brand",
        F.when(F.col("category") == "Electronics", 1).otherwise(0).alias("is_electronics"),
    )
    .orderBy("launch_date", F.col("is_electronics").desc())
)
