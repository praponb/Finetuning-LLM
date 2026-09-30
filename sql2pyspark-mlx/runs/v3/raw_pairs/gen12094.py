from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("brand").orderBy("launch_date", "id")
w2 = Window.partitionBy("category", "supplier_id").orderBy("launch_date", "id")

result = (
    df_products
    .filter(F.col("launch_date") <= "2023-08-20")
    .select(
        "id",
        "brand",
        "launch_date",
        "price",
        "category",
        "supplier_id",
        "cost",
        F.sum("price").over(w).alias("running_price"),
        F.lead("cost", 2).over(w2).alias("next_cost"),
    )
    .orderBy("launch_date")
)
