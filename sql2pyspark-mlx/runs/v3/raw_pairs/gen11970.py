from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id")

result = (
    df_inventory
    .filter(F.col("product_id").isin([1, 2, 5, 8]))
    .select(
        "id",
        "product_id",
        "quantity",
        F.avg("quantity").over(w).alias("product_id_avg_quantity"),
    )
)
