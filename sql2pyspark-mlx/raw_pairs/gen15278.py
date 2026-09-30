from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("category", "brand")
w2 = Window.partitionBy("supplier_id").orderBy("margin", "id").rowsBetween(-3, Window.currentRow)

cleaned = (
    df_products
    .filter(F.col("launch_date") > F.to_date(F.lit("2023-07-13")))
    .select("*", (F.col("cost") * F.col("price")).alias("margin"))
)

result = (
    cleaned
    .select(
        "id",
        "product_name",
        "price",
        "launch_date",
        "tags",
        "supplier_id",
        F.avg("cost").over(w).alias("category_avg_cost"),
        F.avg("cost").over(w2).alias("cost_avg_3"),
    )
)
