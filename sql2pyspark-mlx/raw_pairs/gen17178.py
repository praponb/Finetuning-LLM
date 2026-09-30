from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("category").orderBy("price", "id")

result = (
    df_products
    .select(
        "id",
        "category",
        "price",
        "cost",
        F.first("cost").over(w).alias("first_cost"),
    )
)
