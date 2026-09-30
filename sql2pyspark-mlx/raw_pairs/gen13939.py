from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("price").isNotNull() | (F.col("stock_qty") <= 176))
    .select(
        "id",
        "tags",
        "brand",
        "cost",
        (F.datediff(F.current_date(), "launch_date") / 30).alias("months_since_launch_date"),
    )
)
