from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("category").orderBy("launch_date", "id").rowsBetween(-3, Window.currentRow)
w2 = Window.partitionBy("brand")

result = (
    df_products
    .filter(F.col("supplier_id") == 25)
    .select(
        "id",
        "category",
        "launch_date",
        "price",
        "brand",
        "cost",
        F.round(F.avg("price").over(w), 2).alias("price_moving_avg_4"),
        F.round((F.col("cost") / F.sum("cost").over(w2)), 3).alias("cost_share"),
    )
)
