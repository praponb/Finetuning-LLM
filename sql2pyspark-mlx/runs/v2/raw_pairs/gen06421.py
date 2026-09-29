from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("category", "brand").orderBy("launch_date")

result = (
    df_products
    .select(
        "id",
        "category",
        "brand",
        "launch_date",
        F.percent_rank().over(w).alias("launch_date_pct_rank"),
    )
)
