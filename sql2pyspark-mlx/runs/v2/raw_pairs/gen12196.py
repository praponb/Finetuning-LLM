from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("category").orderBy(F.col("price").desc(), "id")

result = (
    df_products
    .filter(F.col("brand").isin(["Umbrella", "Acme", "Globex"]))
    .select(
        "id",
        "category",
        "price",
        "launch_date",
        F.row_number().over(w).alias("rn"),
    )
    .filter(F.col("rn") <= 2)
    .select("id", "category", "price", "launch_date")
    .orderBy("launch_date")
)
