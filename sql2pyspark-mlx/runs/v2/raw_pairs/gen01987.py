from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("supplier_id")

result = (
    df_products
    .filter(F.col("cost") > 240)
    .select(
        "id",
        "supplier_id",
        "price",
        F.avg("price").over(w).alias("supplier_id_avg_price"),
    )
    .orderBy("supplier_id", F.col("id").desc())
)
