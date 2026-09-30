from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("brand")

result = (
    df_products
    .filter(F.year("launch_date") == 2025)
    .select(
        "id",
        "brand",
        "price",
        F.round((F.col("price") / F.sum("price").over(w)), 3).alias("price_share"),
    )
    .orderBy(F.col("price_share").desc())
)
