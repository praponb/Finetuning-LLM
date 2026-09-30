from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("brand").orderBy("launch_date", "id")

result = (
    df_products
    .filter(F.col("category") != "Electronics")
    .select(
        "id",
        "brand",
        "launch_date",
        "category",
        F.row_number().over(w).alias("rnk"),
    )
    .filter(F.col("rnk") == 1)
    .select("id", "brand", "launch_date", "category")
)
