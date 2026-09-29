from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.partitionBy("supplier_id", "category").orderBy("launch_date", "id").rowsBetween(-6, Window.currentRow)
w2 = Window.partitionBy("supplier_id").orderBy(F.col("cost").desc(), "id")
w3 = Window.partitionBy(F.month("launch_date")).orderBy("stock_qty", "id")

scored = (
    df_products
    .filter(F.col("category") != "Home")
    .select(
        "id",
        "product_name",
        "cost",
        "tags",
        "supplier_id",
        F.avg("price").over(w_grp).alias("price_avg_6"),
        F.ntile(3).over(w2).alias("cost_bucket"),
        F.lead("price", 1).over(w3).alias("next_price"),
    )
)

result = (
    scored
    .select("id", "product_name", "cost", "supplier_id", "price_avg_6")
    .orderBy(F.col("cost").desc_nulls_last())
)
