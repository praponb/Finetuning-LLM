from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("category").orderBy("launch_date", "id")
w2 = Window.partitionBy("supplier_id").orderBy("launch_date", "id")

result = (
    df_products
    .select(
        "id",
        "category",
        "launch_date",
        "cost",
        "supplier_id",
        (F.col("cost") - F.lag("cost", 1).over(w)).alias("cost_change"),
        F.lag("cost", 1).over(w2).alias("prev_cost"),
    )
)
