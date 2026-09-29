from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("quantity", "id")
w2 = Window.partitionBy("product_id").orderBy(F.col("quantity").desc())

result = (
    df_inventory
    .select(
        "id",
        "quantity",
        "product_id",
        F.ntile(3).over(w).alias("quartile"),
        F.rank().over(w2).alias("quantity_rank"),
    )
)
