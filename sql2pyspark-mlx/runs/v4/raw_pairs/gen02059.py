from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("category").orderBy(F.col("launch_date").desc(), "id")

result = (
    df_products
    .filter(F.col("category").isin(["Books", "Grocery"]))
    .select(
        "id",
        "category",
        "launch_date",
        F.row_number().over(w).alias("launch_date_row"),
    )
)
