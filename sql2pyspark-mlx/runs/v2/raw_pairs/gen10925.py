from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("supplier_id", "category").orderBy("launch_date", "id")

result = (
    df_products
    .filter(F.col("stock_qty").between(180, 201))
    .select(
        "id",
        "supplier_id",
        "category",
        "launch_date",
        "cost",
        F.lag("cost", 1, 0).over(w).alias("prev_cost"),
    )
)
