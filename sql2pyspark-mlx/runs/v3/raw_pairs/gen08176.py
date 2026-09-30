from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("category").orderBy("cost", "id")
w2 = Window.partitionBy("brand").orderBy("launch_date")

result = (
    df_products
    .select(
        "id",
        "category",
        "cost",
        "brand",
        "launch_date",
        F.row_number().over(w).alias("row_num"),
        F.rank().over(w2).alias("launch_date_rank"),
    )
)
