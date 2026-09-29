from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("category").orderBy("launch_date", "id").rowsBetween(-6, Window.currentRow)
w2 = Window.partitionBy("category")

result = (
    df_products
    .filter(F.col("product_name").isNotNull())
    .select(
        "id",
        "category",
        "launch_date",
        "cost",
        F.round(F.avg("cost").over(w), 2).alias("cost_moving_avg_7"),
        F.avg("cost").over(w2).alias("category_avg_cost"),
    )
    .orderBy(F.col("category").desc(), F.col("cost_moving_avg_7").desc())
)
