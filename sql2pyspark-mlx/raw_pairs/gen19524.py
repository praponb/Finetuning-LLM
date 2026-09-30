from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("supplier_id")

result = (
    df_products
    .select(
        "id",
        "supplier_id",
        "price",
        F.round((F.col("price") / F.sum("price").over(w)), 3).alias("price_share"),
    )
)
