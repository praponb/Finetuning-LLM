from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.year("launch_date"), "category").orderBy(F.col("cost").desc(), "id")

result = (
    df_products
    .filter(F.col("category").isin(["Books", "Grocery"]))
    .select(
        "id",
        "launch_date",
        "category",
        "cost",
        F.row_number().over(w).alias("cost_row"),
    )
)
