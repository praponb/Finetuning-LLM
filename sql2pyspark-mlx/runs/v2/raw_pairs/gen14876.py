from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("supplier_id").orderBy("launch_date", "id")

result = (
    df_products
    .select(
        "id",
        "supplier_id",
        "launch_date",
        "price",
        F.sum("price").over(w).alias("running_price"),
    )
)
