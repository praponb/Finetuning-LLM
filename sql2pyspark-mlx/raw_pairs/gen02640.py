from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.month("launch_date"), "category").orderBy(F.col("cost").desc(), "id")
w2 = Window.partitionBy("category").orderBy("launch_date", "id")

result = (
    df_products
    .select(
        "*",
        F.ntile(4).over(w).alias("cost_bucket"),
        F.lag("price", 2).over(w2).alias("prev_price"),
    )
)
