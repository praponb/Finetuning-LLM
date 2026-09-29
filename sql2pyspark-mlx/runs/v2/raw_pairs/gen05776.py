from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("category", "brand").orderBy("launch_date", "id")

result = (
    df_products
    .select(
        "id",
        "category",
        "brand",
        "launch_date",
        "price",
        F.lead("price", 1).over(w).alias("next_price"),
    )
)
