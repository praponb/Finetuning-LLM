from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("last_restock").desc(), "id")

result = (
    df_inventory
    .filter(F.col("quantity").between(65, 139))
    .select(
        "id",
        "product_id",
        "last_restock",
        "quantity",
        F.row_number().over(w).alias("rnk"),
    )
    .filter(F.col("rnk") <= 5)
)
