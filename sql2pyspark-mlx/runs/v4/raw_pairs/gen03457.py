from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id")

result = (
    df_inventory
    .select(
        "id",
        "product_id",
        "quantity",
        F.sum("quantity").over(w).alias("product_id_sum_quantity"),
    )
    .orderBy(F.col("id").desc())
)
