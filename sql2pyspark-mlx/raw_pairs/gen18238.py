from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id", "store_id")
w2 = Window.orderBy("last_restock_plus_months", "id")
w3 = Window.partitionBy(F.year("last_restock"), "store_id").orderBy("last_restock_plus_months", "id")

cleaned = (
    df_inventory
    .filter(F.col("last_restock") >= F.lit("2023-02-28").cast("date"))
    .select(
        "id",
        "store_id",
        "product_id",
        "quantity",
        "last_restock",
        F.add_months("last_restock", 6).alias("last_restock_plus_months"),
    )
)

result = (
    cleaned
    .select(
        "id",
        "store_id",
        "product_id",
        F.max("quantity").over(w).alias("product_id_max_quantity"),
        F.lead("quantity", 1).over(w2).alias("next_quantity"),
        F.lead("quantity", 2).over(w3).alias("next_quantity_2"),
    )
)
