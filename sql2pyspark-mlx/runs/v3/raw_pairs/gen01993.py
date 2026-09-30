from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("last_restock").desc())
w2 = Window.orderBy("last_restock", "id")

result = (
    df_inventory
    .filter(F.col("product_id") == 4)
    .select(
        "id",
        "product_id",
        "last_restock",
        "quantity",
        F.rank().over(w).alias("rnk"),
        F.lead("quantity", 1).over(w2).alias("next_quantity"),
    )
    .orderBy(F.col("rnk").desc())
)
