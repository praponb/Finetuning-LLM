from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("supplier_id").orderBy("launch_date", "id")
w2 = Window.partitionBy("brand")

result = (
    df_products
    .filter(F.col("brand").isin(["Acme", "Initech"]))
    .select(
        "id",
        "supplier_id",
        "launch_date",
        "stock_qty",
        "brand",
        "cost",
        (F.col("stock_qty") - F.lag("stock_qty", 1).over(w)).alias("stock_qty_change"),
        F.round((F.col("cost") / F.sum("cost").over(w2)), 3).alias("cost_share"),
    )
)
