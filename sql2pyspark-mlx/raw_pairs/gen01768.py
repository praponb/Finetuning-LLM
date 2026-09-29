from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.year("launch_date"), "brand")

result = (
    df_products
    .filter(F.array_contains("tags", "clearance"))
    .select(
        "id",
        "launch_date",
        "brand",
        "price",
        F.max("price").over(w).alias("launch_date_year_max_price"),
    )
    .orderBy("price")
)
