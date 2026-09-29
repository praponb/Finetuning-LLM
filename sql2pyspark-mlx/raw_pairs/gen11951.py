from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.partitionBy(F.year("launch_date"), "supplier_id")
w2 = Window.partitionBy(F.date_trunc("MONTH", "launch_date"))

result = (
    df_products
    .select(
        "id",
        "category",
        "brand",
        "price",
        "cost",
        "launch_date",
        F.corr("stock_qty", "price").over(w_running).alias("corr_stock_qty_price"),
        F.round((F.col("cost") / F.sum("cost").over(w2)), 4).alias("cost_share"),
    )
)
