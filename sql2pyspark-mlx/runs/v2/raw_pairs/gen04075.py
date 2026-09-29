from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("category").orderBy("launch_date", "id").rowsBetween(-3, Window.currentRow)
w2 = Window.partitionBy("supplier_id").orderBy("launch_date", "id")

result = (
    df_products
    .select(
        "id",
        "category",
        "launch_date",
        "cost",
        "supplier_id",
        "price",
        F.round(F.avg("cost").over(w), 2).alias("cost_moving_avg_4"),
        F.first("price").over(w2).alias("first_price"),
    )
)
