from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("category")
w2 = Window.partitionBy("category", "supplier_id").orderBy("launch_date", "id")

result = (
    df_products
    .select(
        "id",
        "category",
        "cost",
        "supplier_id",
        "launch_date",
        F.sum("cost").over(w).alias("category_sum_cost"),
        F.lag("cost", 1).over(w2).alias("prev_cost"),
    )
)
