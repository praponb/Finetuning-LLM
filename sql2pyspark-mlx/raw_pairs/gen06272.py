from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("brand", "supplier_id").orderBy(F.col("stock_qty").desc_nulls_last())

ranked = (
    df_products
    .withColumn("latest_rank", F.rank().over(w))
    .filter(F.col("latest_rank") == 1)
    .select(
        "id",
        "product_name",
        "price",
        "cost",
        "stock_qty",
        "launch_date",
        "supplier_id",
    )
)

result = (
    ranked
    .select("id", "product_name", "price", "stock_qty")
    .orderBy("id")
)
