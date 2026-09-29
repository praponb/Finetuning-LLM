from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("supplier_id").orderBy("price", "id")

result = (
    df_products
    .filter(F.array_contains("tags", "premium"))
    .select(
        "id",
        "supplier_id",
        "price",
        "cost",
        F.row_number().over(w).alias("row_num"),
    )
    .filter(F.col("row_num") <= 5)
    .orderBy(F.col("price").desc())
)
