from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("category", "brand").orderBy("launch_date", "id").rowsBetween(-6, Window.currentRow)
w2 = Window.partitionBy("brand", "category").orderBy("cost", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("category")

scored = (
    df_products
    .select(
        "id",
        "product_name",
        "category",
        "brand",
        "price",
        "cost",
        "stock_qty",
        "tags",
        F.avg("cost").over(w).alias("cost_avg_6"),
        F.first("cost", ignorenulls=True).over(w2).alias("first_known_cost"),
        F.sum("cost").over(w3).alias("category_sum_cost"),
    )
)

result = (
    scored
    .select("id", "product_name", "category", "tags", "cost_avg_6", "category_sum_cost")
    .orderBy(F.col("id").desc(), "category_sum_cost")
)
