from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("launch_date", "id")
w2 = Window.partitionBy("brand").orderBy("launch_date", "id")

result = (
    df_products
    .filter(F.col("product_name").like("Wa%"))
    .select(
        "id",
        "launch_date",
        "cost",
        "brand",
        "stock_qty",
        F.lead("cost", 1).over(w).alias("next_cost"),
        F.first("stock_qty").over(w2).alias("first_stock_qty"),
    )
)
